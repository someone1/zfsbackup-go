// Copyright © 2016 Prateek Malhotra (someone1@gmail.com)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package backup

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/someone1/zfsbackup-go/backends"
	"github.com/someone1/zfsbackup-go/helpers"
)

type downloadSequence struct {
	object string
	c      chan<- *helpers.VolumeInfo
}

// Algorithm Overview:
// Ask the user for

func Receive(jobInfo *helpers.JobInfo) {
	defer helpers.HandleExit()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer os.RemoveAll(helpers.BackupTempdir)

	// Prepare the backend client
	backend := prepareBackend(ctx, jobInfo, jobInfo.Destinations[0], nil)

	// Get the local cache dir
	localCachePath := getCacheDir(jobInfo.Destinations[0])

	// Compute the Manifest File
	tempManifest, err := helpers.CreateManifestVolume(ctx, jobInfo)
	if err != nil {
		helpers.AppLogger.Errorf("Error trying to create manifest volume - %v", err)
		panic(helpers.Exit{Code: 5})
	}
	tempManifest.Close()
	tempManifest.DeleteVolume()
	safeManifestFile := fmt.Sprintf("%x", md5.Sum([]byte(tempManifest.ObjectName)))
	safeManifestPath := filepath.Join(localCachePath, safeManifestFile)

	// Check to see if we have the manifest file locally
	manifest, err := readManifest(ctx, safeManifestPath, jobInfo)
	if err != nil {
		if os.IsNotExist(err) {
			err = backend.PreDownload([]string{tempManifest.ObjectName})
			if err != nil {
				helpers.AppLogger.Errorf("Error trying to pre download manifest volume - %v", err)
				panic(helpers.Exit{Code: 502})
			}
			// Try and download the manifest file from the backend
			downloadTo(backend, tempManifest.ObjectName, safeManifestPath)
			manifest, err = readManifest(ctx, safeManifestPath, jobInfo)
		}
		if err != nil {
			helpers.AppLogger.Errorf("Error trying to retrieve manifest volume - %v", err)
			panic(helpers.Exit{Code: 501})
		}
	}

	// Get list of Objects
	toDownload := make([]string, len(manifest.Volumes))
	for idx, vol := range manifest.Volumes {
		toDownload[idx] = vol.ObjectName
	}

	// PreDownload step
	err = backend.PreDownload(toDownload)
	if err != nil {
		helpers.AppLogger.Errorf("Error trying to pre download backup set volumes - %v", err)
		panic(helpers.Exit{Code: 503})
	}

	// Prepare Download Pipeline
	usePipe := false
	fileBufferSize := jobInfo.MaxFileBuffer
	if fileBufferSize == 0 {
		fileBufferSize = 1
		if len(manifest.Volumes) > 1 {
			helpers.AppLogger.Errorf("Cannot use a 0 file buffer size when backup set contains more than one volume!")
			panic(helpers.Exit{Code: 504})
		}
		usePipe = true
	}

	downloadChannel := make(chan downloadSequence, len(manifest.Volumes))
	bufferChannel := make(chan interface{}, fileBufferSize)
	orderedChannels := make([]chan *helpers.VolumeInfo, len(toDownload))
	defer close(bufferChannel)

	// Queue up files to download
	for idx, obj := range toDownload {
		c := make(chan *helpers.VolumeInfo, 1)
		orderedChannels[idx] = c
		downloadChannel <- downloadSequence{obj, c}
	}
	close(downloadChannel)

	var wg sync.WaitGroup
	wg.Add(fileBufferSize)

	// Kick off go routines to download
	for i := 0; i < fileBufferSize; i++ {
		go func() {
			buf := make([]byte, 1024*1024)
			defer wg.Done()
			for sequence := range downloadChannel {
				bufferChannel <- nil
				defer close(sequence.c)
				r, rerr := backend.Get(sequence.object)
				if rerr == nil {
					defer r.Close()
					vol, err := helpers.CreateSimpleVolume(ctx, usePipe)
					if err != nil {
						helpers.AppLogger.Errorf("Could not create file due to error - %v.", err)
						panic(helpers.Exit{Code: 205})
					}
					vol.ObjectName = sequence.object
					if usePipe {
						sequence.c <- vol
					}
					_, err = io.CopyBuffer(vol, r, buf)
					if err != nil {
						helpers.AppLogger.Errorf("Could not download file %s to the local cache dir due to error - %v.", sequence.object, err)
						panic(helpers.Exit{Code: 206})
					}
					if !usePipe {
						vol.Close()
						sequence.c <- vol
					}
					helpers.AppLogger.Debugf("Downloaded %s to temporary file.", sequence.object)
				}
			}
		}()
	}

	// Order the downloaded Volumes
	orderedVolumes := make(chan *helpers.VolumeInfo, len(toDownload))
	go func() {
		for _, c := range orderedChannels {
			orderedVolumes <- <-c
		}
		close(orderedVolumes)
	}()

	// Prepare ZFS Receive command
	var rwg sync.WaitGroup
	rwg.Add(1)
	cmd := helpers.GetZFSReceiveCommand(ctx, jobInfo)
	go receiveStream(ctx, cmd, jobInfo, orderedVolumes, bufferChannel, &rwg)

	// Wait for processes to finish
	wg.Wait()
	backend.Close()
	rwg.Wait()

	helpers.AppLogger.Noticef("Done. Elapsed Time: %v", time.Now().Sub(jobInfo.StartTime))
}

func receiveStream(ctx context.Context, cmd *exec.Cmd, j *helpers.JobInfo, c <-chan *helpers.VolumeInfo, buffer <-chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	cin, cout := io.Pipe()
	cmd.Stdin = cin
	cmd.Stderr = os.Stderr

	// Start the zfs receive command
	helpers.AppLogger.Infof("Starting zfs receive command: %s", strings.Join(cmd.Args, " "))
	err := cmd.Start()
	if err != nil {
		helpers.AppLogger.Errorf("Error starting zfs command - %v", err)
		panic(helpers.Exit{Code: 11})
	}

	defer func() {
		if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
			err = cmd.Process.Kill()
			if err != nil {
				helpers.AppLogger.Errorf("Could not kill zfs send command due to error - %v", err)
				return
			}
			err = cmd.Process.Release()
			if err != nil {
				helpers.AppLogger.Errorf("Could not release resources from zfs send command due to error - %v", err)
				return
			}
		}
	}()

	// Extract ZFS stream from files and send it to the zfs command
	go func() {
		buf := make([]byte, 1024*1024)
		for vol := range c {
			helpers.AppLogger.Debugf("Processing %s.", vol.ObjectName)
			err := vol.Extract(ctx, j)
			if err != nil {
				helpers.AppLogger.Errorf("Error while trying to read from volume %s - %v", vol.ObjectName, err)
				panic(helpers.Exit{Code: 507})
			}
			_, err = io.CopyBuffer(cout, vol, buf)
			if err != nil {
				helpers.AppLogger.Errorf("Error while trying to read from volume %s - %v", vol.ObjectName, err)
				panic(helpers.Exit{Code: 508})
			}
			vol.Close()
			vol.DeleteVolume()
			helpers.AppLogger.Debugf("Processed %s.", vol.ObjectName)
			vol = nil
			<-buffer
		}
		cout.Close()
	}()

	// Wait for the command to finish
	err = cmd.Wait()
	if err != nil {
		helpers.AppLogger.Errorf("Error waiting for zfs command to finish - %v", err)
		panic(helpers.Exit{Code: 12})
	}
	helpers.AppLogger.Infof("zfs recieve completed without error")

	return
}

func downloadTo(backend backends.Backend, objectName, toPath string) {
	r, rerr := backend.Get(objectName)
	if rerr == nil {
		defer r.Close()
		out, oerr := os.Create(toPath)
		if oerr != nil {
			helpers.AppLogger.Errorf("Could not create file in the local cache dir due to error - %v.", oerr)
			panic(helpers.Exit{Code: 205})
		}
		defer out.Close()

		_, err := io.Copy(out, r)
		if err != nil {
			helpers.AppLogger.Errorf("Could not download file %s to the local cache dir due to error - %v.", objectName, err)
			panic(helpers.Exit{Code: 206})
		}
		helpers.AppLogger.Debugf("Downloaded %s to local cache.", objectName)
	} else {
		helpers.AppLogger.Errorf("Could not download file %s to the local cache dir due to error - %v.", objectName, rerr)
		panic(helpers.Exit{Code: 208})
	}

}

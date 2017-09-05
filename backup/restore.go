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
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"golang.org/x/sync/errgroup"

	"github.com/someone1/zfsbackup-go/backends"
	"github.com/someone1/zfsbackup-go/helpers"
)

type downloadSequence struct {
	volume *helpers.VolumeInfo
	c      chan<- *helpers.VolumeInfo
}

// AutoRestore will compute which snapshots need to be restored to get to the snapshot provided,
// or to the latest snapshot of the volume provided
func AutoRestore(pctx context.Context, jobInfo *helpers.JobInfo) error {
	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	// Prepare the backend client
	target := jobInfo.Destinations[0]
	backend, berr := prepareBackend(ctx, jobInfo, target, nil)
	if berr != nil {
		helpers.AppLogger.Errorf("Could not initialize backend for target %s due to error - %v.", target, berr)
		return berr
	}
	defer backend.Close()

	// Get the local cache dir
	localCachePath, cerr := getCacheDir(jobInfo.Destinations[0])
	if cerr != nil {
		helpers.AppLogger.Errorf("Could not get cache dir for target %s due to error - %v.", target, cerr)
		return cerr
	}

	// Sync the local cache
	safeManifests, _, serr := syncCache(ctx, jobInfo, localCachePath, backend)
	if serr != nil {
		helpers.AppLogger.Errorf("Could not sync cache dir for target %s due to error - %v.", target, serr)
		return serr
	}

	decodedManifests, derr := readAndSortManifests(ctx, localCachePath, safeManifests, jobInfo)
	if derr != nil {
		return derr
	}
	manifestTree := linkManifests(decodedManifests)
	var ok bool
	var volumeSnaps []*helpers.JobInfo
	if volumeSnaps, ok = manifestTree[jobInfo.VolumeName]; !ok {
		helpers.AppLogger.Errorf("Could not find any snapshots for volume %s, none found on target.", jobInfo.VolumeName)
		return errors.New("could not determine any snapshots for provided volume")
	}

	// Restore to the latest snapshot available for the volume provided if no snapshot was provided
	if jobInfo.BaseSnapshot.Name == "" {
		helpers.AppLogger.Infof("Trying to determine latest snapshot for volume %s.", jobInfo.BaseSnapshot.Name)
		job := volumeSnaps[len(volumeSnaps)-1]
		jobInfo.BaseSnapshot = job.BaseSnapshot
		helpers.AppLogger.Infof("Restoring to snapshot %s.", job.BaseSnapshot.Name)
	}

	// Find the matching backup job for the snapshot we want to restore to
	var jobToRestore *helpers.JobInfo
	for _, job := range volumeSnaps {
		if strings.Compare(job.BaseSnapshot.Name, jobInfo.BaseSnapshot.Name) == 0 {
			jobToRestore = job
			break
		}
	}
	if jobToRestore == nil {
		helpers.AppLogger.Errorf("Could not find the snapshot %v for volume %s on backend.", jobInfo.BaseSnapshot.Name, jobInfo.VolumeName)
		return errors.New("could not find snapshot provided")
	}

	// We have the snapshot we'd like to restore to, let's figure out whats already found locally and restore as required
	jobsToRestore := make([]*helpers.JobInfo, 0, 10)
	helpers.AppLogger.Infof("Calculating how to restore to %s.", jobInfo.BaseSnapshot.Name)
	volume := jobInfo.LocalVolume
	parts := strings.Split(jobInfo.VolumeName, "/")
	if jobInfo.FullPath {
		parts[0] = volume
		volume = strings.Join(parts, "/")
	}

	if jobInfo.LastPath {
		volume = fmt.Sprintf("%s/%s", volume, parts[len(parts)-1])
	}

	snapshots, err := helpers.GetSnapshots(ctx, volume)
	if err != nil {
		// TODO: There are some error cases that are ok to ignore!
		snapshots = []helpers.SnapshotInfo{}
	}

	for {
		// See if the snapshots we want to restore already exist
		if ok := validateSnapShotExistsFromSnaps(&jobToRestore.BaseSnapshot, snapshots); ok {
			break
		}
		helpers.AppLogger.Infof("Adding backup job for %s to the restore list.", jobToRestore.BaseSnapshot.Name)
		jobsToRestore = append(jobsToRestore, jobToRestore)
		if jobToRestore.IncrementalSnapshot.Name == "" {
			// This is a full backup, no need to go further back
			break
		}
		if jobToRestore.ParentSnap == nil {
			helpers.AppLogger.Errorf("Want to restore parent snap %s but it is not found in the backend, aborting.", jobToRestore.IncrementalSnapshot.Name)
			return errors.New("could not find parent snapshot")
		}
		jobToRestore = jobToRestore.ParentSnap
	}

	helpers.AppLogger.Infof("Need to restore %d snapshots.", len(jobsToRestore))

	// We have a list of snapshots we need to restore, start at the end and work our way down
	for i := len(jobsToRestore) - 1; i >= 0; i-- {
		jobInfo.BaseSnapshot = jobsToRestore[i].BaseSnapshot
		jobInfo.IncrementalSnapshot = jobsToRestore[i].IncrementalSnapshot
		jobInfo.Volumes = jobsToRestore[i].Volumes
		jobInfo.Compressor = jobsToRestore[i].Compressor
		jobInfo.Separator = jobsToRestore[i].Separator
		helpers.AppLogger.Infof("Restoring snapshot %s (%d/%d)", jobInfo.BaseSnapshot.Name, len(jobsToRestore)-i, len(jobsToRestore))
		if err := Receive(ctx, jobInfo); err != nil {
			helpers.AppLogger.Errorf("Failed to restore snapshot.")
			return err
		}
	}

	helpers.AppLogger.Noticef("Done.")

	return nil
}

// Receive will download and restore the backup job described to the Volume target provided.
func Receive(pctx context.Context, jobInfo *helpers.JobInfo) error {
	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	target := jobInfo.Destinations[0]

	// Prepare the backend client
	backend, berr := prepareBackend(ctx, jobInfo, target, nil)
	if berr != nil {
		helpers.AppLogger.Errorf("Could not initialize backend for target %s due to error - %v.", target, berr)
		return berr
	}
	defer backend.Close()

	// Get the local cache dir
	localCachePath, cerr := getCacheDir(target)
	if cerr != nil {
		helpers.AppLogger.Errorf("Could not get cache dir for target %s due to error - %v.", target, cerr)
		return cerr
	}

	// See if the snapshots we want to restore already exist
	volume := jobInfo.LocalVolume
	parts := strings.Split(jobInfo.VolumeName, "/")
	if jobInfo.FullPath {
		parts[0] = volume
		volume = strings.Join(parts, "/")
	}

	if jobInfo.LastPath {
		volume = fmt.Sprintf("%s/%s", volume, parts[len(parts)-1])
	}

	if jobInfo.BaseSnapshot.CreationTime.IsZero() {
		if ok, verr := validateSnapShotExists(ctx, &jobInfo.BaseSnapshot, volume); verr != nil {
			helpers.AppLogger.Errorf("Cannot validate if selected base snapshot exists due to error - %v", verr)
			return verr
		} else if ok {
			helpers.AppLogger.Noticef("Selected base snapshot already exists, nothing to do!")
			return nil
		}
	}

	// Check that we have the parent snap shot this wants to restore from
	if jobInfo.IncrementalSnapshot.Name != "" && jobInfo.IncrementalSnapshot.CreationTime.IsZero() {
		if ok, verr := validateSnapShotExists(ctx, &jobInfo.IncrementalSnapshot, volume); verr != nil {
			helpers.AppLogger.Errorf("Cannot validate if selected incremental snapshot exists due to error - %v", verr)
			return verr
		} else if !ok {
			helpers.AppLogger.Errorf("Selected incremental snapshot does not exist!")
			return fmt.Errorf("selected incremental snapshot does not exist")
		}
	}

	// Compute the Manifest File
	tempManifest, err := helpers.CreateManifestVolume(ctx, jobInfo)
	if err != nil {
		helpers.AppLogger.Errorf("Error trying to create manifest volume - %v", err)
		return err
	}
	tempManifest.Close()
	tempManifest.DeleteVolume()
	safeManifestFile := fmt.Sprintf("%x", md5.Sum([]byte(tempManifest.ObjectName)))
	safeManifestPath := filepath.Join(localCachePath, safeManifestFile)

	// Check to see if we have the manifest file locally
	manifest, err := readManifest(ctx, safeManifestPath, jobInfo)
	if err != nil {
		if os.IsNotExist(err) {
			err = backend.PreDownload(ctx, []string{tempManifest.ObjectName})
			if err != nil {
				helpers.AppLogger.Errorf("Error trying to pre download manifest volume - %v", err)
				return err
			}
			// Try and download the manifest file from the backend
			downloadTo(ctx, backend, tempManifest.ObjectName, safeManifestPath)
			manifest, err = readManifest(ctx, safeManifestPath, jobInfo)
		}
		if err != nil {
			helpers.AppLogger.Errorf("Error trying to retrieve manifest volume - %v", err)
			return err
		}
	}

	manifest.ManifestPrefix = jobInfo.ManifestPrefix
	manifest.SignKey = jobInfo.SignKey
	manifest.EncryptKey = jobInfo.EncryptKey

	// Get list of Objects
	toDownload := make([]string, len(manifest.Volumes))
	for idx := range manifest.Volumes {
		toDownload[idx] = manifest.Volumes[idx].ObjectName
	}

	// PreDownload step
	err = backend.PreDownload(ctx, toDownload)
	if err != nil {
		helpers.AppLogger.Errorf("Error trying to pre download backup set volumes - %v", err)
		return err
	}
	toDownload = nil

	// Prepare Download Pipeline
	usePipe := false
	fileBufferSize := jobInfo.MaxFileBuffer
	if fileBufferSize == 0 {
		fileBufferSize = 1
		usePipe = true
	}

	downloadChannel := make(chan downloadSequence, len(manifest.Volumes))
	bufferChannel := make(chan interface{}, fileBufferSize)
	orderedChannels := make([]chan *helpers.VolumeInfo, len(manifest.Volumes))
	defer close(bufferChannel)

	// Queue up files to download
	for idx := range manifest.Volumes {
		c := make(chan *helpers.VolumeInfo, 1)
		orderedChannels[idx] = c
		downloadChannel <- downloadSequence{manifest.Volumes[idx], c}
	}
	close(downloadChannel)

	var wg *errgroup.Group
	wg, ctx = errgroup.WithContext(ctx)

	// Kick off go routines to download
	for i := 0; i < fileBufferSize; i++ {
		wg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case sequence, ok := <-downloadChannel:
					if !ok {
						return nil
					}
					defer close(sequence.c)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case bufferChannel <- nil:
					}

					be := backoff.NewExponentialBackOff()
					be.MaxInterval = jobInfo.MaxBackoffTime
					be.MaxElapsedTime = jobInfo.MaxRetryTime
					retryconf := backoff.WithContext(be, ctx)

					operation := func() error {
						return processSequence(ctx, sequence, backend, usePipe)
					}

					helpers.AppLogger.Debugf("Downloading volume %s.", sequence.volume.ObjectName)

					if berr := backoff.Retry(operation, retryconf); berr != nil {
						helpers.AppLogger.Errorf("Failed to download volume %s due to error: %v, aborting...", sequence.volume.ObjectName, berr)
						return berr
					}
				}
			}
		})
	}

	// Order the downloaded Volumes
	orderedVolumes := make(chan *helpers.VolumeInfo, len(toDownload))
	wg.Go(func() error {
		defer close(orderedVolumes)
		for _, c := range orderedChannels {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case orderedVolumes <- <-c:
				continue
			}
		}
		return nil
	})

	// Prepare ZFS Receive command
	cmd := helpers.GetZFSReceiveCommand(ctx, jobInfo)
	wg.Go(func() error {
		return receiveStream(ctx, cmd, manifest, orderedVolumes, bufferChannel)
	})

	// Wait for processes to finish
	err = wg.Wait()
	if err != nil {
		helpers.AppLogger.Errorf("There was an error during the restore process, aborting: %v", err)
	}

	helpers.AppLogger.Noticef("Done. Elapsed Time: %v", time.Since(jobInfo.StartTime))
	return nil
}

func processSequence(ctx context.Context, sequence downloadSequence, backend backends.Backend, usePipe bool) error {
	r, rerr := backend.Download(ctx, sequence.volume.ObjectName)
	if rerr != nil {
		helpers.AppLogger.Infof("Could not get %s due to error %v.", sequence.volume.ObjectName, rerr)
		return rerr
	}
	defer r.Close()
	vol, err := helpers.CreateSimpleVolume(ctx, usePipe)
	if err != nil {
		helpers.AppLogger.Noticef("Could not create temporary file to download %s due to error - %v.", sequence.volume.ObjectName, err)
		return err
	}

	defer vol.Close()
	vol.ObjectName = sequence.volume.ObjectName
	if usePipe {
		sequence.c <- vol
	}

	_, err = io.Copy(vol, r)
	if err != nil {
		helpers.AppLogger.Noticef("Could not download file %s to the local cache dir due to error - %v.", sequence.volume.ObjectName, err)
		vol.Close()
		vol.DeleteVolume()
		if usePipe {
			return backoff.Permanent(fmt.Errorf("cannot retry when using no file buffer, aborting"))
		}
		return err
	}
	vol.Close()

	// Verify the SHA256 Hash, if it doesn't match, ditch it!
	if vol.SHA256Sum != sequence.volume.SHA256Sum {
		helpers.AppLogger.Infof("Hash mismatch for %s, got %s but expected %s. Retrying.", sequence.volume.ObjectName, vol.SHA256Sum, sequence.volume.SHA256Sum)
		if usePipe {
			return backoff.Permanent(fmt.Errorf("cannot retry when using no file buffer, aborting"))
		}
		vol.DeleteVolume()
		return fmt.Errorf("SHA256 hash mismatch for %s, got %s but expected %s", sequence.volume.ObjectName, vol.SHA256Sum, sequence.volume.SHA256Sum)
	}
	if !usePipe {
		sequence.c <- vol
	}
	helpers.AppLogger.Debugf("Downloaded %s.", sequence.volume.ObjectName)

	return nil
}

func receiveStream(ctx context.Context, cmd *exec.Cmd, j *helpers.JobInfo, c <-chan *helpers.VolumeInfo, buffer <-chan interface{}) error {
	cin, cout := io.Pipe()
	cmd.Stdin = cin
	cmd.Stderr = os.Stderr
	var group *errgroup.Group
	var once sync.Once
	group, ctx = errgroup.WithContext(ctx)

	// Start the zfs receive command
	helpers.AppLogger.Infof("Starting zfs receive command: %s", strings.Join(cmd.Args, " "))
	err := cmd.Start()
	if err != nil {
		helpers.AppLogger.Errorf("Error starting zfs command - %v", err)
		return err
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
	group.Go(func() error {
		defer once.Do(func() { cout.Close() })
		buf := make([]byte, 1024*1024)
		for {
			select {
			case vol, ok := <-c:
				if !ok {
					return nil
				}
				helpers.AppLogger.Debugf("Processing %s.", vol.ObjectName)
				eerr := vol.Extract(ctx, j)
				if eerr != nil {
					helpers.AppLogger.Errorf("Error while trying to read from volume %s - %v", vol.ObjectName, eerr)
					return err
				}
				_, eerr = io.CopyBuffer(cout, vol, buf)
				if eerr != nil {
					helpers.AppLogger.Errorf("Error while trying to read from volume %s - %v", vol.ObjectName, eerr)
					return eerr
				}
				vol.Close()
				vol.DeleteVolume()
				helpers.AppLogger.Debugf("Processed %s.", vol.ObjectName)
				vol = nil
				<-buffer
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	group.Go(func() error {
		defer once.Do(func() { cout.Close() })
		return cmd.Wait()
	})

	// Wait for the command to finish
	err = group.Wait()
	if err != nil {
		helpers.AppLogger.Errorf("Error waiting for zfs command to finish - %v", err)
		return err
	}
	helpers.AppLogger.Infof("zfs receive completed without error")

	return nil
}

func downloadTo(ctx context.Context, backend backends.Backend, objectName, toPath string) error {
	r, rerr := backend.Download(ctx, objectName)
	if rerr == nil {
		defer r.Close()
		out, oerr := os.Create(toPath)
		if oerr != nil {
			helpers.AppLogger.Errorf("Could not create file in the local cache dir due to error - %v.", oerr)
			return oerr
		}
		defer out.Close()

		_, err := io.Copy(out, r)
		if err != nil {
			helpers.AppLogger.Errorf("Could not download file %s to the local cache dir due to error - %v.", objectName, err)
			return err
		}
		helpers.AppLogger.Debugf("Downloaded %s to local cache.", objectName)
	} else {
		helpers.AppLogger.Errorf("Could not download file %s to the local cache dir due to error - %v.", objectName, rerr)
		return rerr
	}
	return nil
}

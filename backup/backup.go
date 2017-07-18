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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/miolini/datacounter"

	"github.com/someone1/zfsbackup-go/backends"
	"github.com/someone1/zfsbackup-go/helpers"
)

// ProcessSmartOptions will compute the snapshots to use
func ProcessSmartOptions(jobInfo *helpers.JobInfo) error {
	snapshots, err := helpers.GetSnapshots(context.Background(), jobInfo.VolumeName)
	if err != nil {
		return err
	}
	jobInfo.BaseSnapshot = snapshots[0]
	if jobInfo.Full {
		// TODO: Check if we already have a full backup for this snapshot in the destination(s)
		return nil
	}
	lastComparableSnapshots := make([]*helpers.SnapshotInfo, len(jobInfo.Destinations))
	lastBackup := make([]*helpers.SnapshotInfo, len(jobInfo.Destinations))
	for idx := range jobInfo.Destinations {
		destBackups, derr := getBackupsForTarget(context.Background(), jobInfo.VolumeName, jobInfo.Destinations[idx], jobInfo)
		if derr != nil {
			return derr
		}
		if len(destBackups) == 0 {
			continue
		}
		lastBackup[idx] = &destBackups[0].BaseSnapshot
		if jobInfo.Incremental {
			lastComparableSnapshots[idx] = &destBackups[0].BaseSnapshot
		}
		if jobInfo.FullIfOlderThan != -1*time.Minute {
			for _, bkp := range destBackups {
				if bkp.IncrementalSnapshot.Name == "" {
					lastComparableSnapshots[idx] = &bkp.BaseSnapshot
					break
				}
			}
		}
	}

	var lastNotEqual bool
	// Verify that all "comparable" snapshots are the same across destinations
	for i := 1; i < len(lastComparableSnapshots); i++ {
		if !lastComparableSnapshots[i-1].Equal(lastComparableSnapshots[i]) {
			return fmt.Errorf("destinations are out of sync, cannot continue with smart option")
		}

		if !lastNotEqual && !lastBackup[i-1].Equal(lastBackup[i]) {
			lastNotEqual = true
		}
	}

	// Now select the proper job options and continue
	if jobInfo.Incremental {
		if lastComparableSnapshots[0] == nil {
			return fmt.Errorf("no snapshot to increment from - try doing a full backup instead")
		}
		if lastComparableSnapshots[0].Equal(&snapshots[0]) {
			return fmt.Errorf("no new snapshot to sync")
		}
		jobInfo.IncrementalSnapshot = *lastComparableSnapshots[0]
	}

	if jobInfo.FullIfOlderThan != -1*time.Minute {
		if lastComparableSnapshots[0] == nil {
			// No previous full backup, so do one
			helpers.AppLogger.Infof("No previous full backup found, performing full backup.")
			return nil
		}
		if snapshots[0].CreationTime.Sub(lastComparableSnapshots[0].CreationTime) > jobInfo.FullIfOlderThan {
			// Been more than the allotted time, do a full backup
			helpers.AppLogger.Infof("Last Full backup was %v and is more than %v before the most recent snapshot, performing full backup.", lastComparableSnapshots[0].CreationTime, jobInfo.FullIfOlderThan)
			return nil
		}
		if lastNotEqual {
			return fmt.Errorf("want to do an incremental backup but last incremental backup at destinations do not match")
		}
		if lastBackup[0].Equal(&snapshots[0]) {
			return fmt.Errorf("no new snapshot to sync")
		}
		jobInfo.IncrementalSnapshot = *lastBackup[0]
	}
	return nil
}

func getBackupsForTarget(ctx context.Context, volume, target string, jobInfo *helpers.JobInfo) ([]*helpers.JobInfo, error) {
	// Prepare the backend client
	backend := prepareBackend(ctx, jobInfo, target, nil)

	// Get the local cache dir
	localCachePath := getCacheDir(target)

	// Sync the local cache
	safeManifests, _ := syncCache(ctx, jobInfo, localCachePath, backend)

	// Read in Manifests and display
	decodedManifests := make([]*helpers.JobInfo, 0, len(safeManifests))
	for _, manifest := range safeManifests {
		manifestPath := filepath.Join(localCachePath, manifest)
		decodedManifest, oerr := readManifest(ctx, manifestPath, jobInfo)
		if oerr != nil {
			return nil, oerr
		}
		if strings.Compare(decodedManifest.VolumeName, volume) == 0 {
			decodedManifests = append(decodedManifests, decodedManifest)
		}
	}

	sort.SliceStable(decodedManifests, func(i, j int) bool {
		return decodedManifests[i].BaseSnapshot.CreationTime.After(decodedManifests[j].BaseSnapshot.CreationTime)
	})
	return decodedManifests, nil
}

// Backup will iniate a backup with the provided configuration.
func Backup(jobInfo *helpers.JobInfo) {
	defer helpers.HandleExit()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer os.RemoveAll(helpers.BackupTempdir)
	safeManifestFile := ""

	// Setup Final Manifest File
	manifest, err := helpers.CreateManifestVolume(ctx, jobInfo)
	if err != nil {
		helpers.AppLogger.Errorf("Error trying to create manifest volume - %v", err)
		panic(helpers.Exit{Code: 5})
	}
	safeManifestFile = fmt.Sprintf("%x", md5.Sum([]byte(manifest.ObjectName)))

	if jobInfo.Resume {
		tryResume(ctx, jobInfo, safeManifestFile)
	}

	fileBufferSize := jobInfo.MaxFileBuffer
	if fileBufferSize == 0 {
		fileBufferSize = 1
	}

	startCh := make(chan *helpers.VolumeInfo, fileBufferSize)
	stepCh := make(chan *helpers.VolumeInfo, fileBufferSize)

	var maniwg sync.WaitGroup
	maniwg.Add(1)

	uploadBuffer := make(chan bool, jobInfo.MaxParallelUploads)
	defer close(uploadBuffer)

	fileBuffer := make(chan bool, fileBufferSize)
	for i := 0; i < fileBufferSize; i++ {
		fileBuffer <- true
	}

	// Used to prevent closing the upload pipeline after the ZFS command is done
	// so we can send the manifest file up.
	go func(next chan<- *helpers.VolumeInfo) {
		defer maniwg.Done()
		for vol := range startCh {
			maniwg.Add(1)
			next <- vol
		}
	}(stepCh)

	cmd := helpers.GetZFSSendCommand(ctx, jobInfo)
	go sendStream(ctx, cmd, jobInfo, startCh, fileBuffer)

	var usedBackends []backends.Backend
	var channels []<-chan *helpers.VolumeInfo
	channels = append(channels, stepCh)

	if jobInfo.MaxFileBuffer != 0 {
		jobInfo.Destinations = append(jobInfo.Destinations, backends.DeleteBackendPrefix)
	}

	for _, destination := range jobInfo.Destinations {
		backend := prepareBackend(ctx, jobInfo, destination, uploadBuffer)
		_ = getCacheDir(destination)
		out := backend.StartUpload(channels[len(channels)-1])
		channels = append(channels, out)
		usedBackends = append(usedBackends, backend)
	}

	go func() {
		defer close(fileBuffer)
		for vol := range channels[len(channels)-1] {
			if !vol.IsManifest {
				maniwg.Done()
				helpers.AppLogger.Debugf("Volume %s has finished the entire pipeline.", vol.ObjectName)
				helpers.AppLogger.Debugf("Adding %s to the manifest volume list.", vol.ObjectName)
				jobInfo.Volumes = append(jobInfo.Volumes, vol)
				// Write a manifest file and save it locally in order to resume later
				if tempManifest, cerr := helpers.CreateManifestVolume(ctx, jobInfo); cerr != nil {
					helpers.AppLogger.Warningf("Unable to write progress manifest file due to an error: %v", cerr)
				} else {
					sort.Sort(helpers.ByVolumeNumber(jobInfo.Volumes))
					jsonEnc := json.NewEncoder(tempManifest)
					err = jsonEnc.Encode(jobInfo)
					if err != nil {
						helpers.AppLogger.Warningf("Could not JSON Encode job information due to error for temporary manifest file - %v", err)
					}
					if err = tempManifest.Close(); err != nil {
						helpers.AppLogger.Warningf("Could not close temporary manifest volume due to error - %v", err)
					}
					for _, destination := range jobInfo.Destinations {
						if destination == backends.DeleteBackendPrefix {
							continue
						}
						safeFolder := fmt.Sprintf("%x", md5.Sum([]byte(destination)))
						dest := filepath.Join(helpers.WorkingDir, "cache", safeFolder, safeManifestFile)
						if err = tempManifest.CopyTo(dest); err != nil {
							helpers.AppLogger.Warningf("Could not write temporary manifest volume due to error - %v", err)
						} else {
							helpers.AppLogger.Debugf("Copied incomplete manifest to local cache for destination %s.", destination)
						}
					}
				}
			}
			select {
			case fileBuffer <- true:
			case <-ctx.Done():
			}
		}
	}()

	maniwg.Wait()
	helpers.AppLogger.Debugf("All volumes dispatched in pipeline, finalizing manifest file.")

	jobInfo.EndTime = time.Now()
	sort.Sort(helpers.ByVolumeNumber(jobInfo.Volumes))
	manifest.IsFinalManifest = true
	jsonEnc := json.NewEncoder(manifest)
	err = jsonEnc.Encode(jobInfo)
	if err != nil {
		helpers.AppLogger.Errorf("Could not JSON Encode job information due to error - %v", err)
		panic(helpers.Exit{Code: 8})
	}
	if err = manifest.Close(); err != nil {
		helpers.AppLogger.Errorf("Could not close manifest volume due to error - %v", err)
		panic(helpers.Exit{Code: 9})
	}
	for _, destination := range jobInfo.Destinations {
		if destination == backends.DeleteBackendPrefix {
			continue
		}
		safeFolder := fmt.Sprintf("%x", md5.Sum([]byte(destination)))
		dest := filepath.Join(helpers.WorkingDir, "cache", safeFolder, safeManifestFile)
		if err = manifest.CopyTo(dest); err != nil {
			helpers.AppLogger.Warningf("Could not write temporary manifest volume due to error - %v", err)
		} else {
			helpers.AppLogger.Debugf("Copied final manifest to local cache for destination %s.", destination)
		}
	}
	stepCh <- manifest
	close(stepCh)

	for _, backend := range usedBackends {
		backend.Wait()
		backend.Close()
	}

	totalWrittenBytes := jobInfo.TotalBytesWritten()
	helpers.AppLogger.Noticef("Done.\n\tTotal ZFS Stream Bytes: %d (%s)\n\tTotal Bytes Written: %d (%s)\n\tElapsed Time: %v\n\tTotal Files Uploaded: %d", jobInfo.ZFSStreamBytes, humanize.IBytes(jobInfo.ZFSStreamBytes), totalWrittenBytes, humanize.IBytes(totalWrittenBytes), time.Now().Sub(jobInfo.StartTime), len(jobInfo.Volumes)+1)
}

func sendStream(ctx context.Context, cmd *exec.Cmd, j *helpers.JobInfo, c chan<- *helpers.VolumeInfo, buffer <-chan bool) {
	cin, cout := io.Pipe()
	cmd.Stdout = cout
	cmd.Stderr = os.Stderr
	counter := datacounter.NewReaderCounter(cin)
	usingPipe := false
	if j.MaxFileBuffer == 0 {
		usingPipe = true
	}

	defer cout.Close() // Is this required?

	go func() {
		var lastTotalBytes uint64
		defer close(c)
		var err error
		var volume *helpers.VolumeInfo
		skipBytes, volNum := j.TotalBytesStreamedAndVols()
		lastTotalBytes = skipBytes
		for {
			if skipBytes > 0 {
				helpers.AppLogger.Debugf("Want to skip %d bytes.", skipBytes)
				written, serr := io.CopyN(ioutil.Discard, counter, int64(skipBytes))
				if serr != nil && serr != io.EOF {
					helpers.AppLogger.Errorf("Error while trying to read from the zfs stream to skip %d bytes - %v", skipBytes, serr)
					panic(helpers.Exit{Code: 16})
				}
				skipBytes -= uint64(written)
				helpers.AppLogger.Debugf("Skipped %d bytes of the ZFS send stream.", written)
				continue
			}
			if volume == nil || volume.Counter() >= (j.VolumeSize*humanize.MiByte)-50*humanize.KiByte {
				if volume != nil {
					helpers.AppLogger.Debugf("Finished creating volume %s", volume.ObjectName)
					volume.ZFSStreamBytes = counter.Count() - lastTotalBytes
					lastTotalBytes = counter.Count()
					if err = volume.Close(); err != nil {
						helpers.AppLogger.Errorf("Error while trying to close volume %s - %v", volume.ObjectName, err)
						panic(helpers.Exit{Code: 13})
					}
					if !usingPipe {
						c <- volume
					}
				}
				<-buffer
				volume, err = helpers.CreateBackupVolume(ctx, j, volNum)
				if err != nil {
					helpers.AppLogger.Errorf("Error while creating volume %d - %v", volNum, err)
					panic(helpers.Exit{Code: 14})
				}
				helpers.AppLogger.Debugf("Starting volume %s", volume.ObjectName)
				volNum++
				if usingPipe {
					c <- volume
				}
			}

			// Write a little at a time and break the output between volumes as needed
			_, ierr := io.CopyN(volume, counter, helpers.BufferSize*2)
			if ierr == io.EOF {
				// We are done!
				helpers.AppLogger.Debugf("Finished creating volume %s", volume.ObjectName)
				volume.ZFSStreamBytes = counter.Count() - lastTotalBytes
				lastTotalBytes = counter.Count()
				if err = volume.Close(); err != nil {
					helpers.AppLogger.Errorf("Error while trying to close volume %s - %v", volume.ObjectName, err)
					panic(helpers.Exit{Code: 15})
				}
				if !usingPipe {
					c <- volume
				}
				return
			} else if ierr != nil {
				helpers.AppLogger.Errorf("Error while trying to read from the zfs stream for volume %s - %v", volume.ObjectName, ierr)
				panic(helpers.Exit{Code: 16})
			}
		}
	}()

	// Start the zfs send command
	helpers.AppLogger.Infof("Starting zfs send command: %s", strings.Join(cmd.Args, " "))
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

	j.ZFSCommandLine = strings.Join(cmd.Args, " ")
	// Wait for the command to finish
	err = cmd.Wait()
	if err != nil {
		helpers.AppLogger.Errorf("Error waiting for zfs command to finish - %v", err)
		panic(helpers.Exit{Code: 12})
	}
	helpers.AppLogger.Infof("zfs send completed without error")
	j.ZFSStreamBytes = counter.Count()

	return
}

func tryResume(ctx context.Context, j *helpers.JobInfo, safeManifestFile string) {
	destination := j.Destinations[0]
	safeFolder := fmt.Sprintf("%x", md5.Sum([]byte(destination)))
	origManiPath := filepath.Join(helpers.WorkingDir, "cache", safeFolder, safeManifestFile)

	if originalManifest, oerr := readManifest(ctx, origManiPath, j); os.IsNotExist(oerr) {
		helpers.AppLogger.Info("No previous manifest file exists, nothing to resume")
	} else if oerr != nil {
		helpers.AppLogger.Errorf("Could not open previous manifest file %s due to error: %v", origManiPath, oerr)
		panic(helpers.Exit{Code: 5})
	} else {
		if originalManifest.Compressor != j.Compressor {
			helpers.AppLogger.Errorf("Cannot resume backup, original compressor %s != compressor specified %s", originalManifest.Compressor, j.Compressor)
			panic(helpers.Exit{Code: 5})
		}

		if originalManifest.EncryptTo != j.EncryptTo {
			helpers.AppLogger.Errorf("Cannot resume backup, different encryptTo flags specified (original %v != current %v)", originalManifest.EncryptTo, j.EncryptTo)
			panic(helpers.Exit{Code: 5})
		}

		if originalManifest.SignFrom != j.SignFrom {
			helpers.AppLogger.Errorf("Cannot resume backup, different signFrom flags specified (original %v != current %v)", originalManifest.SignFrom, j.SignFrom)
			panic(helpers.Exit{Code: 5})
		}

		currentCMD := helpers.GetZFSSendCommand(ctx, j)
		oldCMD := helpers.GetZFSSendCommand(ctx, originalManifest)
		oldCMDLine := strings.Join(currentCMD.Args, " ")
		currentCMDLine := strings.Join(oldCMD.Args, " ")
		if strings.Compare(oldCMDLine, currentCMDLine) != 0 {
			helpers.AppLogger.Errorf("Cannot resume backup, different options given for zfs send command: `%s` != current `%s`", oldCMDLine, currentCMDLine)
			panic(helpers.Exit{Code: 5})
		}

		j.Volumes = originalManifest.Volumes
		j.StartTime = originalManifest.StartTime
		helpers.AppLogger.Infof("Will be resuming previous backup attempt.")
	}
}

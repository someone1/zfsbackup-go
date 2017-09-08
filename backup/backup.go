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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/dustin/go-humanize"
	"github.com/miolini/datacounter"
	"github.com/nightlyone/lockfile"
	"golang.org/x/sync/errgroup"

	"github.com/someone1/zfsbackup-go/backends"
	"github.com/someone1/zfsbackup-go/helpers"
)

var (
	ErrNoOp       = errors.New("nothing new to sync")
	manifestmutex sync.Mutex
)

// ProcessSmartOptions will compute the snapshots to use
func ProcessSmartOptions(ctx context.Context, jobInfo *helpers.JobInfo) error {
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
		destBackups, derr := getBackupsForTarget(ctx, jobInfo.VolumeName, jobInfo.Destinations[idx], jobInfo)
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
			return ErrNoOp
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
			return ErrNoOp
		}

		if ok, verr := validateSnapShotExists(ctx, lastComparableSnapshots[0], jobInfo.VolumeName); verr != nil {
			return verr
		} else if !ok {
			helpers.AppLogger.Infof("Last Full backup was done on %v but is no longer found in the local target, performing full backup.", lastComparableSnapshots[0].CreationTime, jobInfo.FullIfOlderThan)
			return nil
		}
		jobInfo.IncrementalSnapshot = *lastBackup[0]
	}
	return nil
}

// Will list all backups found in the target destination
func getBackupsForTarget(ctx context.Context, volume, target string, jobInfo *helpers.JobInfo) ([]*helpers.JobInfo, error) {
	// Prepare the backend client
	backend, berr := prepareBackend(ctx, jobInfo, target, nil)
	if berr != nil {
		helpers.AppLogger.Errorf("Could not initialize backend due to error - %v.", berr)
		return nil, berr
	}

	// Get the local cache dir
	localCachePath, cerr := getCacheDir(target)
	if cerr != nil {
		helpers.AppLogger.Errorf("Could not get cache dir for target %s due to error - %v.", target, cerr)
		return nil, cerr
	}

	// Sync the local cache
	safeManifests, _, serr := syncCache(ctx, jobInfo, localCachePath, backend)
	if serr != nil {
		helpers.AppLogger.Errorf("Could not sync cache dir for target %s due to error - %v.", target, serr)
		return nil, serr
	}

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

// Backup will initiate a backup with the provided configuration.
func Backup(pctx context.Context, jobInfo *helpers.JobInfo) error {
	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	if jobInfo.Resume {
		if err := tryResume(ctx, jobInfo); err != nil {
			return err
		}
	}

	// Make sure nobody else is working on the same volume/dataset we are!
	lockFilePath := filepath.Join(os.TempDir(), fmt.Sprintf("zfsbackup.%x.lck", md5.Sum([]byte(jobInfo.VolumeName))))
	lock, lferr := lockfile.New(lockFilePath)
	if lferr != nil {
		helpers.AppLogger.Errorf("Cannot init lock. reason: %v", lferr)
		return lferr
	}
	lferr = lock.TryLock()

	if lferr != nil {
		helpers.AppLogger.Errorf("Cannot lock %q, reason: %v. If no other execution of %s is working on %s, you may forcefully remove the lock file located %s.", lock, lferr, helpers.ProgramName, jobInfo.VolumeName, lockFilePath)
		return lferr
	}
	defer lock.Unlock()

	fileBufferSize := jobInfo.MaxFileBuffer
	if fileBufferSize == 0 {
		fileBufferSize = 1
	}

	// Validate the snapshots we want to use exist
	if ok, verr := validateSnapShotExists(ctx, &jobInfo.BaseSnapshot, jobInfo.VolumeName); verr != nil {
		helpers.AppLogger.Errorf("Cannot validate if selected base snapshot exists due to error - %v", verr)
		return verr
	} else if !ok {
		helpers.AppLogger.Errorf("Selected base snapshot does not exist!")
		return fmt.Errorf("selected base snapshot does not exist")
	}

	if jobInfo.IncrementalSnapshot.Name != "" {
		if ok, verr := validateSnapShotExists(ctx, &jobInfo.IncrementalSnapshot, jobInfo.VolumeName); verr != nil {
			helpers.AppLogger.Errorf("Cannot validate if selected incremental snapshot exists due to error - %v", verr)
			return verr
		} else if !ok {
			helpers.AppLogger.Errorf("Selected incremental snapshot does not exist!")
			return fmt.Errorf("selected incremental snapshot does not exist")
		}
	}

	startCh := make(chan *helpers.VolumeInfo, fileBufferSize) // Sent to ZFS command and meant to be closed when done
	stepCh := make(chan *helpers.VolumeInfo, fileBufferSize)  // Used as input to first backend, closed when final manifest is sent through

	var maniwg sync.WaitGroup
	maniwg.Add(1)

	uploadBuffer := make(chan bool, jobInfo.MaxParallelUploads)
	defer close(uploadBuffer)

	fileBuffer := make(chan bool, fileBufferSize)
	for i := 0; i < fileBufferSize; i++ {
		fileBuffer <- true
	}

	var group *errgroup.Group
	group, ctx = errgroup.WithContext(ctx)

	// Used to prevent closing the upload pipeline after the ZFS command is done
	// so we can send the manifest file up after all volumes have made it to the backends.
	go func() {
		defer maniwg.Done()
		for {
			select {
			case vol, ok := <-startCh:
				if !ok {
					return
				}
				maniwg.Add(1)
				select {
				// Might take a while to pass along the volume so be sure to listen to context cancellations
				case stepCh <- vol:
					continue
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start the ZFS send stream
	group.Go(func() error {
		return sendStream(ctx, jobInfo, startCh, fileBuffer)
	})

	var usedBackends []backends.Backend
	var channels []<-chan *helpers.VolumeInfo
	channels = append(channels, stepCh)

	if jobInfo.MaxFileBuffer != 0 {
		jobInfo.Destinations = append(jobInfo.Destinations, backends.DeleteBackendPrefix+"://")
	}

	// Prepare backends and setup plumbing
	for _, destination := range jobInfo.Destinations {
		backend, berr := prepareBackend(ctx, jobInfo, destination, uploadBuffer)
		if berr != nil {
			helpers.AppLogger.Errorf("Could not initialize backend due to error - %v.", berr)
			return berr
		}
		_, cerr := getCacheDir(destination)
		if cerr != nil {
			helpers.AppLogger.Errorf("Could not create cache for destination %s due to error - %v.", destination, cerr)
			return cerr
		}
		out, waitgroup := retryUploadChainer(ctx, channels[len(channels)-1], backend, jobInfo, destination)
		channels = append(channels, out)
		usedBackends = append(usedBackends, backend)
		group.Go(waitgroup.Wait)
	}

	// Create and copy a copy of the manifest during the backup procedure for future retry requests
	group.Go(func() error {
		defer close(fileBuffer)
		lastChan := channels[len(channels)-1]
		for {
			select {
			case vol, ok := <-lastChan:
				if !ok {
					return nil
				}
				if !vol.IsManifest {
					helpers.AppLogger.Debugf("Volume %s has finished the entire pipeline.", vol.ObjectName)
					helpers.AppLogger.Debugf("Adding %s to the manifest volume list.", vol.ObjectName)
					manifestmutex.Lock()
					jobInfo.Volumes = append(jobInfo.Volumes, vol)
					manifestmutex.Unlock()
					// Write a manifest file and save it locally in order to resume later
					manifestVol, err := saveManifest(ctx, jobInfo, false)
					if err != nil {
						return err
					}
					if err = manifestVol.DeleteVolume(); err != nil {
						helpers.AppLogger.Warningf("Error deleting temporary manifest file  - %v", err)
					}
					maniwg.Done()
				} else {
					// Manifest has been processed, we're done!
					return nil
				}
				select {
				// May take a while to add to buffer channel so listen for context cancellations.
				case <-ctx.Done():
					return ctx.Err()

				case fileBuffer <- true:
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Final Manifest Creation
	group.Go(func() error {
		// TODO: How to incorporate contexts in this go routine?
		maniwg.Wait() // Wait until the ZFS send command has completed and all volumes have been uploaded to all backends.
		helpers.AppLogger.Infof("All volumes dispatched in pipeline, finalizing manifest file.")
		manifestmutex.Lock()
		jobInfo.EndTime = time.Now()
		manifestmutex.Unlock()
		manifestVol, err := saveManifest(ctx, jobInfo, true)
		if err != nil {
			return err
		}
		stepCh <- manifestVol
		close(stepCh)
		return nil
	})

	err := group.Wait() // Wait for ZFS Send to finish, Backends to finish, and Manifest files to be copied/uploaded
	if err != nil {
		return err
	}

	totalWrittenBytes := jobInfo.TotalBytesWritten()
	if helpers.JSONOutput {
		var doneOutput = struct {
			TotalZFSBytes    uint64
			TotalBackupBytes uint64
			ElapsedTime      time.Duration
			FilesUploaded    int
		}{jobInfo.ZFSStreamBytes, totalWrittenBytes, time.Since(jobInfo.StartTime), len(jobInfo.Volumes) + 1}
		if j, jerr := json.Marshal(doneOutput); jerr != nil {
			helpers.AppLogger.Errorf("could not ouput json due to error - %v", jerr)
		} else {
			fmt.Fprintf(helpers.Stdout, "%s", string(j))
		}
	} else {
		fmt.Fprintf(helpers.Stdout, "Done.\n\tTotal ZFS Stream Bytes: %d (%s)\n\tTotal Bytes Written: %d (%s)\n\tElapsed Time: %v\n\tTotal Files Uploaded: %d", jobInfo.ZFSStreamBytes, humanize.IBytes(jobInfo.ZFSStreamBytes), totalWrittenBytes, humanize.IBytes(totalWrittenBytes), time.Since(jobInfo.StartTime), len(jobInfo.Volumes)+1)
	}

	helpers.AppLogger.Debugf("Cleaning up resources...")

	for _, backend := range usedBackends {
		if err = backend.Close(); err != nil {
			helpers.AppLogger.Warningf("Could not properly close backend due to error - %v", err)
		}
	}

	return nil
}

func saveManifest(ctx context.Context, j *helpers.JobInfo, final bool) (*helpers.VolumeInfo, error) {
	manifestmutex.Lock()
	defer manifestmutex.Unlock()
	sort.Sort(helpers.ByVolumeNumber(j.Volumes))

	// Setup Manifest File
	manifest, err := helpers.CreateManifestVolume(ctx, j)
	if err != nil {
		helpers.AppLogger.Errorf("Error trying to create manifest volume - %v", err)
		return nil, err
	}
	safeManifestFile := fmt.Sprintf("%x", md5.Sum([]byte(manifest.ObjectName)))
	manifest.IsFinalManifest = final
	jsonEnc := json.NewEncoder(manifest)
	err = jsonEnc.Encode(j)
	if err != nil {
		helpers.AppLogger.Errorf("Could not JSON Encode job information due to error - %v", err)
		return nil, err
	}
	if err = manifest.Close(); err != nil {
		helpers.AppLogger.Errorf("Could not close manifest volume due to error - %v", err)
		return nil, err
	}
	for _, destination := range j.Destinations {
		if destination == backends.DeleteBackendPrefix+"://" {
			continue
		}
		safeFolder := fmt.Sprintf("%x", md5.Sum([]byte(destination)))
		dest := filepath.Join(helpers.WorkingDir, "cache", safeFolder, safeManifestFile)
		if err = manifest.CopyTo(dest); err != nil {
			helpers.AppLogger.Warningf("Could not write manifest volume due to error - %v", err)
			return nil, err
		}
		helpers.AppLogger.Debugf("Copied manifest to local cache for destination %s.", destination)
	}
	return manifest, nil
}

func sendStream(ctx context.Context, j *helpers.JobInfo, c chan<- *helpers.VolumeInfo, buffer <-chan bool) error {
	var group *errgroup.Group
	group, ctx = errgroup.WithContext(ctx)

	cmd := helpers.GetZFSSendCommand(ctx, j)
	cin, cout := io.Pipe()
	cmd.Stdout = cout
	cmd.Stderr = os.Stderr
	counter := datacounter.NewReaderCounter(cin)
	usingPipe := false
	if j.MaxFileBuffer == 0 {
		usingPipe = true
	}

	group.Go(func() error {
		var lastTotalBytes uint64
		defer close(c)
		var err error
		var volume *helpers.VolumeInfo
		skipBytes, volNum := j.TotalBytesStreamedAndVols()
		lastTotalBytes = skipBytes
		for {
			// Skip bytes if we are resuming
			if skipBytes > 0 {
				helpers.AppLogger.Debugf("Want to skip %d bytes.", skipBytes)
				written, serr := io.CopyN(ioutil.Discard, counter, int64(skipBytes))
				if serr != nil && serr != io.EOF {
					helpers.AppLogger.Errorf("Error while trying to read from the zfs stream to skip %d bytes - %v", skipBytes, serr)
					return serr
				}
				skipBytes -= uint64(written)
				helpers.AppLogger.Debugf("Skipped %d bytes of the ZFS send stream.", written)
				continue
			}

			// Setup next Volume
			if volume == nil || volume.Counter() >= (j.VolumeSize*humanize.MiByte)-50*humanize.KiByte {
				if volume != nil {
					helpers.AppLogger.Debugf("Finished creating volume %s", volume.ObjectName)
					volume.ZFSStreamBytes = counter.Count() - lastTotalBytes
					lastTotalBytes = counter.Count()
					if err = volume.Close(); err != nil {
						helpers.AppLogger.Errorf("Error while trying to close volume %s - %v", volume.ObjectName, err)
						return err
					}
					if !usingPipe {
						c <- volume
					}
				}
				<-buffer
				volume, err = helpers.CreateBackupVolume(ctx, j, volNum)
				if err != nil {
					helpers.AppLogger.Errorf("Error while creating volume %d - %v", volNum, err)
					return err
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
					return err
				}
				if !usingPipe {
					c <- volume
				}
				return nil
			} else if ierr != nil {
				helpers.AppLogger.Errorf("Error while trying to read from the zfs stream for volume %s - %v", volume.ObjectName, ierr)
				return ierr
			}
		}
	})

	// Start the zfs send command
	helpers.AppLogger.Infof("Starting zfs send command: %s", strings.Join(cmd.Args, " "))
	err := cmd.Start()
	if err != nil {
		helpers.AppLogger.Errorf("Error starting zfs command - %v", err)
		return err
	}

	group.Go(func() error {
		defer cout.Close()
		return cmd.Wait()
	})

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

	manifestmutex.Lock()
	j.ZFSCommandLine = strings.Join(cmd.Args, " ")
	manifestmutex.Unlock()
	// Wait for the command to finish

	err = group.Wait()
	if err != nil {
		helpers.AppLogger.Errorf("Error waiting for zfs command to finish - %v", err)
		return err
	}
	helpers.AppLogger.Infof("zfs send completed without error")
	manifestmutex.Lock()
	j.ZFSStreamBytes = counter.Count()
	manifestmutex.Unlock()
	return nil
}

func tryResume(ctx context.Context, j *helpers.JobInfo) error {
	// Temproary Final Manifest File
	manifest, merr := helpers.CreateManifestVolume(ctx, j)
	if merr != nil {
		helpers.AppLogger.Errorf("Error trying to create manifest volume - %v", merr)
		return merr
	}
	defer manifest.DeleteVolume()
	defer manifest.Close()

	safeManifestFile := fmt.Sprintf("%x", md5.Sum([]byte(manifest.ObjectName)))

	destination := j.Destinations[0]
	safeFolder := fmt.Sprintf("%x", md5.Sum([]byte(destination)))
	origManiPath := filepath.Join(helpers.WorkingDir, "cache", safeFolder, safeManifestFile)

	if originalManifest, oerr := readManifest(ctx, origManiPath, j); os.IsNotExist(oerr) {
		helpers.AppLogger.Info("No previous manifest file exists, nothing to resume")
	} else if oerr != nil {
		helpers.AppLogger.Errorf("Could not open previous manifest file %s due to error: %v", origManiPath, oerr)
		return oerr
	} else {
		if originalManifest.Compressor != j.Compressor {
			helpers.AppLogger.Errorf("Cannot resume backup, original compressor %s != compressor specified %s", originalManifest.Compressor, j.Compressor)
			return fmt.Errorf("option mismatch")
		}

		if originalManifest.EncryptTo != j.EncryptTo {
			helpers.AppLogger.Errorf("Cannot resume backup, different encryptTo flags specified (original %v != current %v)", originalManifest.EncryptTo, j.EncryptTo)
			return fmt.Errorf("option mismatch")
		}

		if originalManifest.SignFrom != j.SignFrom {
			helpers.AppLogger.Errorf("Cannot resume backup, different signFrom flags specified (original %v != current %v)", originalManifest.SignFrom, j.SignFrom)
			return fmt.Errorf("option mismatch")
		}

		currentCMD := helpers.GetZFSSendCommand(ctx, j)
		oldCMD := helpers.GetZFSSendCommand(ctx, originalManifest)
		oldCMDLine := strings.Join(currentCMD.Args, " ")
		currentCMDLine := strings.Join(oldCMD.Args, " ")
		if strings.Compare(oldCMDLine, currentCMDLine) != 0 {
			helpers.AppLogger.Errorf("Cannot resume backup, different options given for zfs send command: `%s` != current `%s`", oldCMDLine, currentCMDLine)
			return fmt.Errorf("option mismatch")
		}

		manifestmutex.Lock()
		j.Volumes = originalManifest.Volumes
		j.StartTime = originalManifest.StartTime
		manifestmutex.Unlock()
		helpers.AppLogger.Infof("Will be resuming previous backup attempt.")
	}
	return nil
}

func retryUploadChainer(ctx context.Context, in <-chan *helpers.VolumeInfo, b backends.Backend, j *helpers.JobInfo, dest string) (<-chan *helpers.VolumeInfo, *errgroup.Group) {
	out := make(chan *helpers.VolumeInfo)
	parts := strings.Split(dest, "://")
	prefix := parts[0]
	var gwg *errgroup.Group
	if j.MaxParallelUploads > 1 {
		gwg, ctx = errgroup.WithContext(ctx)
	} else {
		gwg = new(errgroup.Group)
	}

	var wg sync.WaitGroup
	wg.Add(j.MaxParallelUploads)
	for i := 0; i < j.MaxParallelUploads; i++ {
		gwg.Go(func() error {
			defer wg.Done()
			for vol := range in {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					helpers.AppLogger.Debugf("%s backend: Processing volume %s", prefix, vol.ObjectName)
					// Prepare the backoff retryer (forces the user configured retry options across all backends)
					be := backoff.NewExponentialBackOff()
					be.MaxInterval = j.MaxBackoffTime
					be.MaxElapsedTime = j.MaxRetryTime
					retryconf := backoff.WithContext(be, ctx)

					operation := volUploadWrapper(ctx, b, vol, prefix)
					if err := backoff.Retry(operation, retryconf); err != nil {
						helpers.AppLogger.Errorf("%s backend: Failed to upload volume %s due to error: %v", prefix, vol.ObjectName, err)
						return err
					}
					helpers.AppLogger.Debugf("%s backend: Processed volume %s", prefix, vol.ObjectName)
					out <- vol
				}
			}
			return nil
		})
	}

	gwg.Go(func() error {
		wg.Wait()
		helpers.AppLogger.Debugf("%s backend: closing out channel.", prefix)
		close(out)
		return nil
	})

	return out, gwg
}

func volUploadWrapper(ctx context.Context, b backends.Backend, vol *helpers.VolumeInfo, prefix string) func() error {
	return func() error {
		if err := vol.OpenVolume(); err != nil {
			helpers.AppLogger.Warningf("%s: Error while opening volume %s - %v", prefix, vol.ObjectName, err)
			return err
		}
		defer vol.Close()

		err := b.Upload(ctx, vol)
		if err != nil {
			helpers.AppLogger.Warningf("%s: Error while uploading volume %s - %v", prefix, vol.ObjectName, err)
		}
		return err
	}
}

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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"golang.org/x/sync/errgroup"

	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
)

// Clean will remove files found in the desination that are not found in any of the manifests found locally or in the destination.
// If cleanLocal is true, then local manifests not found in the destination are ignored and deleted. This function will optionally
// delete broken backup sets in the destination if the --force flag is provided.
func Clean(pctx context.Context, jobInfo *files.JobInfo, cleanLocal bool) error {
	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	// Prepare the backend client
	target := jobInfo.Destinations[0]
	backend, berr := prepareBackend(ctx, jobInfo, target, nil)
	if berr != nil {
		log.AppLogger.Errorf("Could not initialize backend for target %s due to error - %v.", target, berr)
		return berr
	}
	defer backend.Close()

	// Get the local cache dir
	localCachePath, cerr := getCacheDir(target)
	if cerr != nil {
		log.AppLogger.Errorf("Could not get cache dir for target %s due to error - %v.", target, cerr)
		return cerr
	}

	// Sync the local cache
	safeManifests, localOnlyFiles, serr := syncCache(ctx, jobInfo, localCachePath, backend)
	if serr != nil {
		log.AppLogger.Errorf("Could not sync cache dir for target %s due to error - %v.", target, serr)
		return serr
	}

	// Read in Manifests
	decodedManifests := make([]*files.JobInfo, 0, len(safeManifests))
	for _, manifest := range safeManifests {
		manifestPath := filepath.Join(localCachePath, manifest)
		decodedManifest, oerr := readManifest(ctx, manifestPath, jobInfo)
		if oerr != nil {
			log.AppLogger.Errorf("Could not read manifest %s due to error - %v", manifestPath, oerr)
			return oerr
		}
		decodedManifests = append(decodedManifests, decodedManifest)
	}

	if !cleanLocal {
		if len(localOnlyFiles) > 0 {
			log.AppLogger.Noticef("There are %d local manifests not found in the destination, use --cleanLocal to delete these locally and any of their volumes found in the destination.", len(localOnlyFiles))
			for _, manifest := range localOnlyFiles {
				manifestPath := filepath.Join(localCachePath, manifest)
				decodedManifest, oerr := readManifest(ctx, manifestPath, jobInfo)
				if oerr != nil {
					log.AppLogger.Errorf("Could not read manifest %s due to error - %v", manifestPath, oerr)
					return oerr
				}
				decodedManifests = append(decodedManifests, decodedManifest)
			}
		}
	} else {
		for _, manifest := range localOnlyFiles {
			manifestPath := filepath.Join(localCachePath, manifest)
			err := os.Remove(manifestPath)
			if err != nil {
				log.AppLogger.Errorf("Could not delete local manifest %s due to error - %v", manifestPath, err)
				return err
			}
			log.AppLogger.Debugf("Deleted %s.", manifestPath)
		}
	}

	// TODO: The following can be done in a much more efficient way (probably)
	allObjects, err := backend.List(ctx, "")
	if err != nil {
		log.AppLogger.Errorf("Could not list objects in backend %s due to error - %v", target, err)
		return err
	}

	// Remove Manifest Files
	for idx := 0; idx < len(allObjects); idx++ {
		if strings.HasPrefix(allObjects[idx], jobInfo.ManifestPrefix) {
			allObjects = append(allObjects[:idx], allObjects[idx+1:]...)
			idx--
		}
	}

	// Go through all manifests and remove from the allObjects list what we know should exist
	for _, manifest := range decodedManifests {
		for vidx, vol := range manifest.Volumes {
			found := false
			for idx := range allObjects {
				if strings.Compare(vol.ObjectName, allObjects[idx]) == 0 {
					allObjects = append(allObjects[:idx], allObjects[idx+1:]...)
					found = true
					break
				}
			}

			if !found {
				// Broken backup set! inform the user!
				if jobInfo.Force {
					log.AppLogger.Warningf("The following backup set is missing volume %s. Removing entire backupset:\n\n%s", vol.ObjectName, manifest.String())

					// Compute the manifest object name and cache name to delete
					manifest.ManifestPrefix = jobInfo.ManifestPrefix
					manifest.SignKey = jobInfo.SignKey
					manifest.EncryptKey = jobInfo.EncryptKey
					tempManifest, terr := files.CreateManifestVolume(ctx, manifest)
					if terr != nil {
						log.AppLogger.Errorf("Could not compute manifest path due to error - %v.", terr)
						return terr
					}
					allObjects = append(allObjects, tempManifest.ObjectName)
					tempManifest.Close()
					tempManifest.DeleteVolume()
					manifestPath := filepath.Join(localCachePath, fmt.Sprintf("%x", md5.Sum([]byte(tempManifest.ObjectName))))
					err = os.Remove(manifestPath)
					if err != nil {
						log.AppLogger.Errorf("Could not delete local manifest %s due to error - %v. Continuing.", manifestPath, err)
					}

					// Delete all volumes already processed in the manifest
					for i := 0; i < vidx; i++ {
						allObjects = append(allObjects, manifest.Volumes[i].ObjectName)
					}
					break
				} else {
					log.AppLogger.Warningf("The following backup set is missing volume %s:\n\n%s\n\nPass the --force flag to delete this backup set.", vol.ObjectName, manifest.String())
				}
			}
		}
	}

	log.AppLogger.Noticef("Starting to delete %d objects in destination.", len(allObjects))

	// Whatever is left in allObjects was not found in any manifest, delete 'em
	var group *errgroup.Group
	group, ctx = errgroup.WithContext(ctx)

	deleteChan := make(chan string, len(allObjects))
	for _, obj := range allObjects {
		deleteChan <- obj
	}
	close(deleteChan)

	// Let's not slam the endpoint with a lot of concurrent requests, pick a sensible default and stick to it
	for i := 0; i < 5; i++ {
		group.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case objectPath, ok := <-deleteChan:
					if !ok {
						return nil
					}

					be := backoff.NewExponentialBackOff()
					be.MaxInterval = time.Minute
					be.MaxElapsedTime = 10 * time.Minute
					retryconf := backoff.WithContext(be, ctx)

					operation := func() error {
						return backend.Delete(ctx, objectPath)
					}

					if berr := backoff.Retry(operation, retryconf); berr != nil {
						log.AppLogger.Errorf("Could not delete object %s in due to error - %v", objectPath, berr)
						return berr
					}

					log.AppLogger.Debugf("Deleted %s.", filepath.Join(target, objectPath))
				}
			}
		})
	}

	log.AppLogger.Debugf("Waiting to delete %d objects in destination.", len(allObjects))
	err = group.Wait()
	if err != nil {
		log.AppLogger.Errorf("Could not finish clean operation due to error, aborting: %v", err)
		return err
	}

	log.AppLogger.Noticef("Done.")
	return nil
}

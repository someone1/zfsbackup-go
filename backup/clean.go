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
	"sync"
	"github.com/someone1/zfsbackup-go/helpers"
)

// Clean will remove fiels found in the desination that are not found in any of the manifests found locally or in the destination.
// If cleanLocal is true, then local manifests not found in the destination are ignored and deleted. This function will optionally
// delete broken backup sets in the destination if the --force flag is provided.
func Clean(jobInfo *helpers.JobInfo, cleanLocal bool) {
	defer helpers.HandleExit()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare the backend client
	backend := prepareBackend(ctx, jobInfo, jobInfo.Destinations[0], nil)

	// Get the local cache dir
	localCachePath := getCacheDir(jobInfo.Destinations[0])

	// Sync the local cache
	safeManifests, localOnlyFiles := syncCache(ctx, jobInfo, localCachePath, backend)

	// Read in Manifests
	decodedManifests := make([]*helpers.JobInfo, 0, len(safeManifests))
	for _, manifest := range safeManifests {
		manifestPath := filepath.Join(localCachePath, manifest)
		decodedManifest, oerr := readManifest(ctx, manifestPath, jobInfo)
		if oerr != nil {
			helpers.AppLogger.Errorf("Could not read manifest %s due to error - %v", manifestPath, oerr)
			panic(helpers.Exit{Code: 301})
		}
		decodedManifests = append(decodedManifests, decodedManifest)
	}

	if !cleanLocal {
		if len(localOnlyFiles) > 0 {
			helpers.AppLogger.Noticef("There are %d local manifests not found in the destination, use --cleanLocal to delete these locally and any of their volumes found in the destination.", len(localOnlyFiles))
			for _, manifest := range localOnlyFiles {
				manifestPath := filepath.Join(localCachePath, manifest)
				decodedManifest, oerr := readManifest(ctx, manifestPath, jobInfo)
				if oerr != nil {
					helpers.AppLogger.Errorf("Could not read manifest %s due to error - %v", manifestPath, oerr)
					panic(helpers.Exit{Code: 302})
				}
				decodedManifests = append(decodedManifests, decodedManifest)
			}
		}
	} else {
		for _, manifest := range localOnlyFiles {
			manifestPath := filepath.Join(localCachePath, manifest)
			err := os.Remove(manifestPath)
			if err != nil {
				helpers.AppLogger.Errorf("Could not delete local manifest %s due to error - %v", manifestPath, err)
				panic(helpers.Exit{Code: 303})
			}
			helpers.AppLogger.Debugf("Deleted %s.", manifestPath)
		}

	}

	// TODO: The following can be done in a much more efficient way (probably)

	allObjects, err := backend.List("")
	if err != nil {
		helpers.AppLogger.Errorf("Could not list objects in backend %s due to error - %v", jobInfo.Destinations[0], err)
		panic(helpers.Exit{Code: 304})
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
					helpers.AppLogger.Warningf("The following backup set is missing volume %s. Removing entire backupset:\n\n%s", vol.ObjectName, manifest.String())

					// Compute the manifest object name and cache name to delete
					manifest.ManifestPrefix = jobInfo.ManifestPrefix
					manifest.SignKey = jobInfo.SignKey
					manifest.EncryptKey = jobInfo.EncryptKey
					tempManifest, err := helpers.CreateManifestVolume(ctx, manifest)
					if err != nil {
						helpers.AppLogger.Errorf("Could not compute manifest path due to error - %v.", err)
						panic(helpers.Exit{Code: 306})
					}
					allObjects = append(allObjects, tempManifest.ObjectName)
					tempManifest.Close()
					tempManifest.DeleteVolume()
					manifestPath := filepath.Join(localCachePath, fmt.Sprintf("%x", md5.Sum([]byte(tempManifest.ObjectName))))
					err = os.Remove(manifestPath)
					if err != nil {
						helpers.AppLogger.Errorf("Could not delete local manifest %s due to error - %v. Continuing.", manifestPath, err)
					}

					// Delete all volumes already processed in the manifest
					for i := 0; i < vidx; i++ {
						allObjects = append(allObjects, manifest.Volumes[i].ObjectName)
					}
					break
				} else {
					helpers.AppLogger.Warningf("The following backup set is missing volume %s:\n\n%s\n\nPass the --force flag to delete this backup set.", vol.ObjectName, manifest.String())
				}
			}
		}
	}

	helpers.AppLogger.Noticef("Starting to delete %d objects in destination.", len(allObjects))

	// Whatever is left in allObjects was not found in any manifest, delete 'em
	var wg sync.WaitGroup
	wg.Add(len(allObjects))

	// Let's not try and do too many deletes at once!
	buffer := make(chan interface{}, 20)
	defer close(buffer)

	for _, obj := range allObjects {
		go func(objectPath string) {
			buffer <- nil
			defer wg.Done()
			err := backend.Delete(objectPath)
			if err != nil {
				helpers.AppLogger.Errorf("Could not delete object %s in due to error - %v", objectPath, err)
				panic(helpers.Exit{Code: 305})
			}
			helpers.AppLogger.Debugf("Deleted %s.", filepath.Join(jobInfo.Destinations[0], objectPath))
			<-buffer
		}(obj)
	}

	helpers.AppLogger.Debugf("Waiting to delete %d objects in destination.", len(allObjects))
	wg.Wait()

	helpers.AppLogger.Noticef("Done.")
}

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
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/someone1/zfsbackup-go/config"
	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
)

// List will sync the manifests found in the target destination to the local cache
// and then read and output the manifest information describing the backup sets
// found in the target destination.
// TODO: Group by volume name?
func List(pctx context.Context, jobInfo *files.JobInfo, startswith string, before, after time.Time) error {
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
	localCachePath, cerr := getCacheDir(jobInfo.Destinations[0])
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

	decodedManifests, derr := readAndSortManifests(ctx, localCachePath, safeManifests, jobInfo)
	if derr != nil {
		return derr
	}

	// Filter Manifests to only results we care about
	filteredResults := decodedManifests[:0]
	for _, manifest := range decodedManifests {
		if startswith != "" {
			if startswith[len(startswith)-1:] == "*" {
				if len(startswith) != 1 && !strings.HasPrefix(manifest.VolumeName, startswith[:len(startswith)-1]) {
					continue
				}
			} else if strings.Compare(startswith, manifest.VolumeName) != 0 {
				continue
			}
		}

		if !before.IsZero() && !manifest.BaseSnapshot.CreationTime.Before(before) {
			continue
		}

		if !after.IsZero() && !manifest.BaseSnapshot.CreationTime.After(after) {
			continue
		}

		filteredResults = append(filteredResults, manifest)
	}

	decodedManifests = filteredResults

	if !config.JSONOutput {
		var output []string

		output = append(output, fmt.Sprintf("Found %d backup sets:\n", len(decodedManifests)))
		for _, manifest := range decodedManifests {
			output = append(output, manifest.String())
		}

		if len(localOnlyFiles) > 0 {
			output = append(output, fmt.Sprintf("There are %d manifests found locally that are not on the target destination.", len(localOnlyFiles)))
			localOnlyOuput := []string{"The following manifests were found locally and can be removed using the clean command."}
			for _, filename := range localOnlyFiles {
				manifestPath := filepath.Join(localCachePath, filename)
				decodedManifest, derr := readManifest(ctx, manifestPath, jobInfo)
				if derr != nil {
					log.AppLogger.Warningf("Could not read local only manifest %s due to error %v", manifestPath, derr)
					continue
				}
				localOnlyOuput = append(localOnlyOuput, decodedManifest.String())
			}
			log.AppLogger.Infof(strings.Join(localOnlyOuput, "\n"))
		}
		fmt.Fprintln(config.Stdout, strings.Join(output, "\n"))
	} else {
		organizedManifests := linkManifests(decodedManifests)
		j, jerr := json.Marshal(organizedManifests)
		if jerr != nil {
			log.AppLogger.Errorf("could not marshal results to JSON - %v", jerr)
			return jerr
		}

		fmt.Fprintln(config.Stdout, string(j))
	}

	return nil
}

func readAndSortManifests(ctx context.Context, localCachePath string, manifests []string, jobInfo *files.JobInfo) ([]*files.JobInfo, error) {
	// Read in Manifests and display
	decodedManifests := make([]*files.JobInfo, 0, len(manifests))
	for _, manifest := range manifests {
		manifestPath := filepath.Join(localCachePath, manifest)
		decodedManifest, oerr := readManifest(ctx, manifestPath, jobInfo)
		if oerr != nil {
			log.AppLogger.Errorf("Could not read manifest %s due to error - %v", manifestPath, oerr)
			return nil, oerr
		}
		decodedManifests = append(decodedManifests, decodedManifest)
	}

	sort.SliceStable(decodedManifests, func(i, j int) bool {
		cmp := strings.Compare(decodedManifests[i].VolumeName, decodedManifests[j].VolumeName)
		if cmp == 0 {
			return decodedManifests[i].BaseSnapshot.CreationTime.Before(decodedManifests[j].BaseSnapshot.CreationTime)
		}
		return cmp < 0

	})

	return decodedManifests, nil
}

// linkManifests will group manifests by Volume and link parents to their children
func linkManifests(manifests []*files.JobInfo) map[string][]*files.JobInfo {
	if manifests == nil {
		return nil
	}
	manifestTree := make(map[string][]*files.JobInfo)
	manifestsByID := make(map[string]*files.JobInfo)
	for idx := range manifests {
		key := manifests[idx].VolumeName

		manifestID := fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%s%s%v", key, manifests[idx].BaseSnapshot.Name, manifests[idx].BaseSnapshot.CreationTime))))

		manifestTree[key] = append(manifestTree[key], manifests[idx])

		// Case 1: Full Backups, nothing to link
		if manifests[idx].IncrementalSnapshot.Name == "" {
			// We will always assume full backups are ideal when selecting a parent
			manifestsByID[manifestID] = manifests[idx]
		} else if _, ok := manifestsByID[manifestID]; !ok {
			// Case 2: Incremental Backup - only make it the designated parent if we haven't gone one already
			manifestsByID[manifestID] = manifests[idx]
		}
	}

	// Link up parents
	for _, snapList := range manifestTree {
		for _, val := range snapList {
			if val.IncrementalSnapshot.Name == "" {
				// Full backup, no parent
				continue
			}
			manifestID := fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%s%s%v", val.VolumeName, val.IncrementalSnapshot.Name, val.IncrementalSnapshot.CreationTime))))
			if psnap, ok := manifestsByID[manifestID]; ok {
				val.ParentSnap = psnap
			} else {
				log.AppLogger.Warningf("Could not find matching parent for %v", val)
			}
		}

	}
	return manifestTree
}

func readManifest(ctx context.Context, manifestPath string, j *files.JobInfo) (*files.JobInfo, error) {
	decodedManifest := new(files.JobInfo)
	manifestVol, err := files.ExtractLocal(ctx, j, manifestPath, true)
	if err != nil {
		return nil, err
	}
	defer manifestVol.Close()
	decoder := json.NewDecoder(manifestVol)
	err = decoder.Decode(decodedManifest)
	if err != nil {
		return nil, err
	}

	return decodedManifest, nil
}

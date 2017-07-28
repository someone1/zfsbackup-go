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
	"path/filepath"

	"github.com/someone1/zfsbackup-go/helpers"
)

// Verify will ensure the provided backupset exists in the target destination.
func Verify(pctx context.Context, jobInfo *helpers.JobInfo) error {
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

	// Read in Manifests
	decodedManifests := make([]*helpers.JobInfo, 0, len(safeManifests))
	for _, manifest := range safeManifests {
		manifestPath := filepath.Join(localCachePath, manifest)
		decodedManifest, oerr := readManifest(ctx, manifestPath, jobInfo)
		if oerr != nil {
			helpers.AppLogger.Errorf("Could not read manifest %s due to error - %v", manifestPath, oerr)
			return oerr
		}
		decodedManifests = append(decodedManifests, decodedManifest)
	}

	// TODO: Finish this....
	return nil
}

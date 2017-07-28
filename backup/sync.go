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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/someone1/zfsbackup-go/backends"
	"github.com/someone1/zfsbackup-go/helpers"
)

func prepareBackend(ctx context.Context, j *helpers.JobInfo, backendURI string, uploadBuffer chan bool) (backends.Backend, error) {
	helpers.AppLogger.Debugf("Initializing Backend %s", backendURI)
	conf := &backends.BackendConfig{
		MaxParallelUploadBuffer: uploadBuffer,
		TargetURI:               backendURI,
		MaxParallelUploads:      j.MaxParallelUploads,
		MaxBackoffTime:          j.MaxBackoffTime,
		MaxRetryTime:            j.MaxRetryTime,
	}

	backend, err := backends.GetBackendForURI(backendURI)
	if err != nil {
		return nil, err
	}

	err = backend.Init(ctx, conf)

	return backend, err
}

func getCacheDir(backendURI string) (string, error) {
	safeFolder := fmt.Sprintf("%x", md5.Sum([]byte(backendURI)))
	dest := filepath.Join(helpers.WorkingDir, "cache", safeFolder)
	oerr := os.MkdirAll(dest, os.ModePerm)
	if oerr != nil {
		return "", fmt.Errorf("could not create cache directory %s due to an error: %v", dest, oerr)
	}

	return dest, nil
}

// Returns local manifest paths that exist in the backend and those that do not
func syncCache(ctx context.Context, j *helpers.JobInfo, localCache string, backend backends.Backend) ([]string, []string, error) {
	// List all manifests at the destination
	manifests, merr := backend.List(ctx, j.ManifestPrefix)
	if merr != nil {
		return nil, nil, fmt.Errorf("could not list manifest files from the backed due to error - %v", merr)
	}

	// Make it safe for local file system storage
	safeManifests := make([]string, len(manifests))
	for idx := range manifests {
		safeManifests[idx] = fmt.Sprintf("%x", md5.Sum([]byte(manifests[idx])))
	}

	// Check what manifests we have locally, and if we are missing any, download them
	files, ferr := ioutil.ReadDir(localCache)
	if ferr != nil {
		return nil, nil, fmt.Errorf("could not list files from the local cache dir due to error - %v", ferr)
	}

	var localOnlyFiles []string
	var foundFiles []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		found := false
		for idx := range manifests {
			if strings.Compare(file.Name(), safeManifests[idx]) == 0 {
				found = true
				foundFiles = append(foundFiles, safeManifests[idx])
				manifests = append(manifests[:idx], manifests[idx+1:]...)
				safeManifests = append(safeManifests[:idx], safeManifests[idx+1:]...)
				break
			}
		}
		if !found {
			localOnlyFiles = append(localOnlyFiles, file.Name())
		}
	}

	pderr := backend.PreDownload(ctx, manifests)
	if pderr != nil {
		return nil, nil, fmt.Errorf("could not prepare manifests for download due to error - %v", pderr)
	}

	if len(manifests) > 0 {
		helpers.AppLogger.Debugf("Syncing %d manifests to local cache.", len(manifests))

		// manifests should only contain what we don't have locally
		for idx, manifest := range manifests {
			downloadTo(ctx, backend, manifest, filepath.Join(localCache, safeManifests[idx]))
		}
	}

	safeManifests = append(safeManifests, foundFiles...)

	return safeManifests, localOnlyFiles, nil
}

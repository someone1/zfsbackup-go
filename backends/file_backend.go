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

package backends

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
)

// FileBackendPrefix is the URI prefix used for the FileBackend.
const FileBackendPrefix = "file"

// FileBackend provides a local destination storage option.
type FileBackend struct {
	conf      *BackendConfig
	localPath string
}

// Init will initialize the FileBackend and verify the provided URI is valid/exists.
func (f *FileBackend) Init(ctx context.Context, conf *BackendConfig, opts ...Option) error {
	f.conf = conf

	cleanPrefix := strings.TrimPrefix(f.conf.TargetURI, FileBackendPrefix+"://")
	if cleanPrefix == f.conf.TargetURI {
		return ErrInvalidURI
	}

	absLocalPath, err := filepath.Abs(cleanPrefix)
	if err != nil {
		log.AppLogger.Errorf("file backend: Error while verifying path %s - %v", cleanPrefix, err)
		return err
	}

	fi, err := os.Stat(absLocalPath)
	if err != nil {
		log.AppLogger.Errorf("file backend: Error while verifying path %s - %v", absLocalPath, err)
		return err
	}

	if !fi.IsDir() {
		log.AppLogger.Errorf("file backend: Provided path is not a directory!")
		return ErrInvalidURI
	}

	f.localPath = absLocalPath
	return nil
}

// Upload will copy the provided VolumeInfo to the backend's configured local destination
func (f *FileBackend) Upload(ctx context.Context, vol *files.VolumeInfo) error {
	f.conf.MaxParallelUploadBuffer <- true
	defer func() {
		<-f.conf.MaxParallelUploadBuffer
	}()

	destinationPath := filepath.Join(f.localPath, vol.ObjectName)
	destinationDir := filepath.Dir(destinationPath)

	if err := os.MkdirAll(destinationDir, os.ModePerm); err != nil {
		log.AppLogger.Debugf("file backend: Could not create path %s due to error - %v", destinationDir, err)
		return err
	}

	w, err := os.Create(destinationPath)
	if err != nil {
		log.AppLogger.Debugf("file backend: Could not create file %s due to error - %v", destinationPath, err)
		return err
	}

	_, err = io.Copy(w, vol)
	if err != nil {
		if closeErr := w.Close(); closeErr != nil {
			log.AppLogger.Warningf("file backend: Error closing volume %s - %v", vol.ObjectName, closeErr)
		}
		if deleteErr := os.Remove(destinationPath); deleteErr != nil {
			log.AppLogger.Warningf("file backend: Error deleting failed upload file %s - %v", destinationPath, deleteErr)
		}
		log.AppLogger.Debugf("file backend: Error while copying volume %s - %v", vol.ObjectName, err)
		return err
	}

	return w.Close()
}

// Delete will delete the given object from the provided path
func (f *FileBackend) Delete(ctx context.Context, filename string) error {
	return os.Remove(filepath.Join(f.localPath, filename))
}

// PreDownload does nothing on this backend.
func (f *FileBackend) PreDownload(ctx context.Context, objects []string) error {
	return nil
}

// Download will open the file for reading
func (f *FileBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(f.localPath, filename))
}

// Close does nothing for this backend.
func (f *FileBackend) Close() error {
	return nil
}

// List will return a list of all files matching the provided prefix
func (f *FileBackend) List(ctx context.Context, prefix string) ([]string, error) {
	l := make([]string, 0, 1000)
	err := filepath.Walk(f.localPath, func(path string, fi os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}

		trimmedPath := strings.TrimPrefix(path, f.localPath+string(filepath.Separator))
		if !fi.IsDir() && strings.HasPrefix(trimmedPath, prefix) {
			l = append(l, trimmedPath)
		}
		return nil
	})

	return l, err
}

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
	"sync"

	"github.com/someone1/zfsbackup-go/helpers"
)

// FileBackendPrefix is the URI prefix used for the FileBackend.
const FileBackendPrefix = "file"

// FileBackend provides a local destination storage option.
type FileBackend struct {
	conf      *BackendConfig
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	localPath string
}

// Init will initialize the FileBackend and verify the provided URI is valid/exists.
func (f *FileBackend) Init(ctx context.Context, conf *BackendConfig) error {
	f.conf = conf
	f.ctx, f.cancel = context.WithCancel(ctx)

	cleanPrefix := strings.TrimPrefix(f.conf.TargetURI, "file://")
	if cleanPrefix == f.conf.TargetURI {
		return ErrInvalidURI
	}

	absLocalPath, err := filepath.Abs(cleanPrefix)
	if err != nil {
		helpers.AppLogger.Errorf("file backend: Error while verifying path %s - %v", cleanPrefix, err)
		return ErrInvalidURI
	}

	fi, err := os.Stat(absLocalPath)
	if err != nil {
		helpers.AppLogger.Errorf("file backend: Error while verifying path %s - %v", absLocalPath, err)
		return ErrInvalidURI
	}

	if !fi.IsDir() {
		helpers.AppLogger.Errorf("file backend: Provided path is not a directory!")
		return ErrInvalidURI
	}

	f.localPath = absLocalPath
	return nil
}

// StartUpload will begin the file copy workers
func (f *FileBackend) StartUpload(in <-chan *helpers.VolumeInfo) <-chan *helpers.VolumeInfo {
	out := make(chan *helpers.VolumeInfo)
	f.wg.Add(f.conf.MaxParallelUploads)
	for i := 0; i < f.conf.MaxParallelUploads; i++ {
		go func() {
			uploader(f.ctx, f.uploadWrapper, "file", f.conf.getExpBackoff(), in, out)
			f.wg.Done()
		}()
	}

	go func() {
		f.Wait()
		close(out)
	}()

	return out
}

func (f *FileBackend) uploadWrapper(vol *helpers.VolumeInfo) func() error {
	return func() error {
		select {
		case <-f.ctx.Done():
			return nil
		default:
			f.conf.MaxParallelUploadBuffer <- true
			defer func() {
				<-f.conf.MaxParallelUploadBuffer
			}()

			if err := vol.OpenVolume(); err != nil {
				helpers.AppLogger.Debugf("file backend: Error while opening volume %s - %v", vol.ObjectName, err)
				return err
			}
			defer vol.Close()
			destinationPath := filepath.Join(f.localPath, vol.ObjectName)
			destinationDir := filepath.Dir(destinationPath)

			if err := os.MkdirAll(destinationDir, os.ModePerm); err != nil {
				helpers.AppLogger.Debugf("file backend: Could not create path %s due to error - %v", destinationDir, err)
				return nil
			}

			w, err := os.Create(destinationPath)
			if err != nil {
				helpers.AppLogger.Debugf("file backend: Could not create file %s due to error - %v", destinationPath, err)
				return nil
			}

			_, err = io.Copy(w, vol)
			if err != nil {
				helpers.AppLogger.Debugf("file backend: Error while copying volume %s - %v", vol.ObjectName, err)
				// Check if the context was canceled, and if so, don't let the backoff function retry
				select {
				case <-f.ctx.Done():
					return nil
				default:
				}
			}
			return err
		}
	}
}

// Delete will delete the given object from the provided path
func (f *FileBackend) Delete(filename string) error {
	return os.Remove(filepath.Join(f.localPath, filename))
}

// PreDownload does nothing on this backend.
func (f *FileBackend) PreDownload(objects []string) error {
	return nil
}

// Get will open the file for reading
func (f *FileBackend) Get(filename string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(f.localPath, filename))
}

// Wait will wait until all volumes have been processed from the incoming
// channel.
func (f *FileBackend) Wait() {
	f.wg.Wait()
}

// Close will signal the upload workers to stop processing the contents of the incoming channel
// and to close the outgoing channel. It will also cancel any ongoing requests.
func (f *FileBackend) Close() error {
	f.cancel()
	f.Wait()
	return nil
}

// List will return a list of all files matching the provided prefix
func (f *FileBackend) List(prefix string) ([]string, error) {
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

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
	"errors"
	"io"
	"time"

	"github.com/cenk/backoff"

	"github.com/someone1/zfsbackup-go/helpers"
)

// Backend is an interface type that defines the functions and functionality required for different backend implementations.
// It is required that if the OutgoingVolumes channel is non-nil, that the backend send every recieved *helpers.VolumeInfo from the IncomingVolumes
// channel to the OutgoingVolumes channel only when the backend is
type Backend interface {
	Init(ctx context.Context, conf *BackendConfig) error                  // Verifies settings required for backend are present and valid, does basic initialization of backend
	StartUpload(in <-chan *helpers.VolumeInfo) <-chan *helpers.VolumeInfo // Tells the backend that we will begin the upload process and to ready any upload workers listening on the provided channel. It should return a channel that sends anything that was sent to it after it is 100% done processing it and has released all locks on it.
	List(prefix string) ([]string, error)                                 // Lists all files in the backend
	Wait()                                                                // Wait on all operations to complete, including operations queued up
	Close() error                                                         // Cancel any oustanding operations and release any resources in use
	PreDownload(objects []string) error                                   // PreDownload will prepare the provided files for download (think restoring from Glacier to S3)
	Get(filename string) (io.ReadCloser, error)                           // Download the requested file that can be read from the returned ReaderCloser
	Delete(filename string) error                                         // Delete the file specified on the configured backend
}

// BackendConfig holds values that relate to backend configurations
type BackendConfig struct {
	MaxParallelUploadBuffer chan bool
	MaxParallelUploads      int
	MaxBackoffTime          time.Duration
	MaxRetryTime            time.Duration
	TargetURI               string
	ManifestPrefix          string
}

var (
	// ErrInvalidURI is returned when a backend determines that the provided URI is malformed/invalid.
	ErrInvalidURI = errors.New("backends: invalid URI provided to backend")
	// ErrInvalidPrefix is returned when a backend destination is provided with a URI prefix that isn't registered.
	ErrInvalidPrefix = errors.New("backends: the provided prefix does not exist")
)

func (b *BackendConfig) getExpBackoff() backoff.BackOff {
	be := backoff.NewExponentialBackOff()
	be.MaxInterval = b.MaxBackoffTime
	be.MaxElapsedTime = b.MaxRetryTime
	return be
}

// GetBackendForPrefix will return the backend associated with the provided prefix.
func GetBackendForPrefix(prefix string) (Backend, error) {
	switch prefix {
	case DeleteBackendPrefix:
		return &DeleteBackend{}, nil
	case GoogleCloudStorageBackendPrefix:
		return &GoogleCloudStorageBackend{}, nil
	case AWSS3BackendPrefix:
		return &AWSS3Backend{}, nil
	default:
		return nil, ErrInvalidPrefix
	}
}

type uploadWrapper func(vol *helpers.VolumeInfo) func() error

func uploader(ctx context.Context, u uploadWrapper, prefix string, b backoff.BackOff, in <-chan *helpers.VolumeInfo, out chan<- *helpers.VolumeInfo) {
	for vol := range in {
		select {
		case <-ctx.Done():
			return
		default:
			helpers.AppLogger.Debugf("%s backend: Uploading volume %s", prefix, vol.ObjectName)
			operation := u(vol)
			if err := backoff.Retry(operation, b); err != nil {
				// TODO: How to handle errors!?
				helpers.AppLogger.Errorf("%s backend: Failed to upload volume %s due to error: %v", prefix, vol.ObjectName, err)
				panic(helpers.Exit{Code: 17})
			}
			helpers.AppLogger.Debugf("%s backend: Uploaded volume %s", prefix, vol.ObjectName)
			out <- vol
		}
	}
}

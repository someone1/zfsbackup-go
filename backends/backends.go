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
	"strings"
	"time"

	"github.com/someone1/zfsbackup-go/helpers"
)

// Backend is an interface type that defines the functions and functionality required for different backend implementations.
// It is required that if the OutgoingVolumes channel is non-nil, that the backend send every recieved *helpers.VolumeInfo from the IncomingVolumes
// channel to the OutgoingVolumes channel only when the backend is
type Backend interface {
	Init(ctx context.Context, conf *BackendConfig, opts ...Option) error  // Verifies settings required for backend are present and valid, does basic initialization of backend
	Upload(ctx context.Context, vol *helpers.VolumeInfo) error            // Upload the volume provided
	List(ctx context.Context, prefix string) ([]string, error)            // Lists all files in the backend
	Close() error                                                         // Release any resources in use
	PreDownload(ctx context.Context, objects []string) error              // PreDownload will prepare the provided files for download (think restoring from Glacier to S3)
	Download(ctx context.Context, filename string) (io.ReadCloser, error) // Download the requested file that can be read from the returned ReaderCloser
	Delete(ctx context.Context, filename string) error                    // Delete the file specified on the configured backend
}

// Option lets users inject functionality to specific backends
type Option interface {
	Apply(Backend)
}

// BackendConfig holds values that relate to backend configurations
type BackendConfig struct {
	MaxParallelUploadBuffer chan bool
	MaxParallelUploads      int
	MaxBackoffTime          time.Duration
	MaxRetryTime            time.Duration
	TargetURI               string
}

var (
	// ErrInvalidURI is returned when a backend determines that the provided URI is malformed/invalid.
	ErrInvalidURI = errors.New("backends: invalid URI provided to backend")
	// ErrInvalidPrefix is returned when a backend destination is provided with a URI prefix that isn't registered.
	ErrInvalidPrefix = errors.New("backends: the provided prefix does not exist")
)

// GetBackendForURI will try and parse the URI for a matching backend to use.
func GetBackendForURI(uri string) (Backend, error) {
	prefix := strings.Split(uri, "://")
	if len(prefix) < 2 {
		return nil, ErrInvalidURI
	}

	switch prefix[0] {
	case DeleteBackendPrefix:
		return &DeleteBackend{}, nil
	case GoogleCloudStorageBackendPrefix:
		return &GoogleCloudStorageBackend{}, nil
	case AWSS3BackendPrefix:
		return &AWSS3Backend{}, nil
	case FileBackendPrefix:
		return &FileBackend{}, nil
	default:
		return nil, ErrInvalidPrefix
	}
}

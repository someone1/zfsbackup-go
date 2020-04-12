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

	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
)

// DeleteBackendPrefix is the URI prefix used for the DeleteBackend.
const DeleteBackendPrefix = "delete"

// DeleteBackend is a special Backend used to delete the files after they've been uploaded
type DeleteBackend struct {
	conf *BackendConfig
}

// Init will initialize the DeleteBackend (aka do nothing)
func (d *DeleteBackend) Init(ctx context.Context, conf *BackendConfig, opts ...Option) error {
	d.conf = conf
	return nil
}

// Delete should not be used with this backend
func (d *DeleteBackend) Delete(ctx context.Context, filename string) error {
	return errors.New("delete backend: Delete is invalid for this backend")
}

// PreDownload should not be used with this backend
func (d *DeleteBackend) PreDownload(ctx context.Context, objects []string) error {
	return errors.New("delete backend: PreDownload is invalid for this backend")
}

// Download should not be used with this backend
func (d *DeleteBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	return nil, errors.New("delete backend: Get is invalid for this backend")
}

// Close does nothing for this backend.
func (d *DeleteBackend) Close() error {
	return nil
}

// List should not be used with this backend
func (d *DeleteBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, errors.New("delete backend: List is invalid for this backend")
}

// Upload will delete the provided volume, usually found in a temporary folder
func (d *DeleteBackend) Upload(ctx context.Context, vol *files.VolumeInfo) error {
	if err := vol.DeleteVolume(); err != nil {
		log.AppLogger.Errorf("delete backend: could not delete volume %s due to error: %v", vol.ObjectName, err)
		return err
	}
	log.AppLogger.Debugf("delete backend: Deleted Volume %s", vol.ObjectName)

	return nil
}

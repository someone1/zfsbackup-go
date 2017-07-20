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

	"golang.org/x/sync/errgroup"

	"github.com/someone1/zfsbackup-go/helpers"
)

// DeleteBackendPrefix is the URI prefix used for the DeleteBackend.
const DeleteBackendPrefix = "delete"

// DeleteBackend is a special Backend used to delete the files after they've been uploaded
type DeleteBackend struct {
	conf *BackendConfig
	wg   *errgroup.Group
}

// Init will initialize the DeleteBackend
func (d *DeleteBackend) Init(ctx context.Context, conf *BackendConfig) error {
	d.conf = conf
	return nil
}

func (d *DeleteBackend) listener(ctx context.Context, in <-chan *helpers.VolumeInfo, out chan<- *helpers.VolumeInfo) error {
	for vol := range in {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := vol.DeleteVolume(); err != nil {
				helpers.AppLogger.Errorf("delete backend: could not delete volume %s due to error: %v", vol.ObjectName, err)
				return err
			}
			helpers.AppLogger.Debugf("delete backend: Deleted Volume %s", vol.ObjectName)
			select {
			case <-ctx.Done():
				continue
			default:
				out <- vol
			}
		}
	}
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

// Get should not be used with this backend
func (d *DeleteBackend) Get(ctx context.Context, filename string) (io.ReadCloser, error) {
	return nil, errors.New("delete backend: Get is invalid for this backend")
}

// Wait will wait until all volumes have been deleted and processed from the incoming
// channel.
func (d *DeleteBackend) Wait() error {
	if d.wg != nil {
		return d.wg.Wait()
	}
	return nil
}

// Close will signal the listener to stop processing the contents of the incoming channel
// and to close the outgoing channel.
func (d *DeleteBackend) Close() error {
	_ = d.Wait()
	return nil
}

// List should not be used with this backend
func (d *DeleteBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, errors.New("delete backend: List is invalid for this backend")
}

// StartUpload will run the channel listener on the incoming channel
func (d *DeleteBackend) StartUpload(ctx context.Context, in <-chan *helpers.VolumeInfo) <-chan *helpers.VolumeInfo {
	d.wg, ctx = errgroup.WithContext(ctx)
	out := make(chan *helpers.VolumeInfo)
	d.wg.Go(func() error {
		return d.listener(ctx, in, out)
	})
	d.wg.Go(func() error {
		_ = d.Wait()
		helpers.AppLogger.Debugf("delete backend: closing out channel.")
		close(out)
		return nil
	})
	return out
}

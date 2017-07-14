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
	"sync"

	"github.com/someone1/zfsbackup-go/helpers"
)

// DeleteBackendPrefix is the URI prefix used for the DeleteBackend.
const DeleteBackendPrefix = "delete"

// DeleteBackend is a special Backend used to delete the files after they've been uploaded
type DeleteBackend struct {
	conf   *BackendConfig
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// Init will initialize the DeleteBackend
func (d *DeleteBackend) Init(ctx context.Context, conf *BackendConfig) error {
	d.conf = conf
	d.ctx, d.cancel = context.WithCancel(ctx)

	return nil
}

func (d *DeleteBackend) listener(in <-chan *helpers.VolumeInfo, out chan<- *helpers.VolumeInfo) {
	defer d.wg.Done()
	for {
		select {
		case <-d.ctx.Done():
			return
		case vol := <-in:
			if vol == nil {
				return
			}
			if err := vol.DeleteVolume(); err != nil {
				helpers.AppLogger.Errorf("delete backend: could not delete volume %s due to error: %v", vol.ObjectName, err)
				panic(helpers.Exit{Code: 12})
			}
			helpers.AppLogger.Debugf("delete backend: Deleted Volume %s", vol.ObjectName)
			out <- vol
		}
	}
}

// Delete should not be used with this backend
func (d *DeleteBackend) Delete(filename string) error {
	return errors.New("delete backend: Delete is invalid for this backend")
}

// PreDownload should not be used with this backend
func (d *DeleteBackend) PreDownload(objects []string) error {
	return errors.New("delete backend: PreDownload is invalid for this backend")
}

// Get should not be used with this backend
func (d *DeleteBackend) Get(filename string) (io.ReadCloser, error) {
	return nil, errors.New("delete backend: Get is invalid for this backend")
}

// Wait will wait until all volumes have been deleted and processed from the incoming
// channel.
func (d *DeleteBackend) Wait() {
	d.wg.Wait()
}

// Close will signal the listener to stop processing the contents of the incoming channel
// and to close the outgoing channel.
func (d *DeleteBackend) Close() error {
	d.cancel()
	return nil
}

// List should not be used with this backend
func (d *DeleteBackend) List(prefix string) ([]string, error) {
	return nil, errors.New("delete backend: List is invalid for this backend")
}

// StartUpload will run the channel listener on the incoming channel
func (d *DeleteBackend) StartUpload(in <-chan *helpers.VolumeInfo) <-chan *helpers.VolumeInfo {
	d.wg.Add(1)
	out := make(chan *helpers.VolumeInfo)
	go d.listener(in, out)
	go func() {
		d.Wait()
		close(out)
	}()
	return out
}

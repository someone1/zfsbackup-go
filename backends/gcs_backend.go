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
	"fmt"
	"io"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/someone1/zfsbackup-go/helpers"
)

// GoogleCloudStorageBackendPrefix is the URI prefix used for the GoogleCloudStorageBackend.
const GoogleCloudStorageBackendPrefix = "gs"

// Authenticate: https://developers.google.com/identity/protocols/application-default-credentials

// GoogleCloudStorageBackend integrates with Google Cloud Storage.
type GoogleCloudStorageBackend struct {
	conf       *BackendConfig
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	client     *storage.Client
	prefix     string
	bucketName string
}

// Init will initialize the GoogleCloudStorageBackend and verify the provided URI is valid/exists.
func (d *GoogleCloudStorageBackend) Init(ctx context.Context, conf *BackendConfig) error {
	d.conf = conf
	d.ctx, d.cancel = context.WithCancel(ctx)

	cleanPrefix := strings.TrimPrefix(d.conf.TargetURI, "gs://")
	if cleanPrefix == d.conf.TargetURI {
		return ErrInvalidURI
	}

	uriParts := strings.Split(cleanPrefix, "/")
	if len(uriParts) < 1 {
		return ErrInvalidURI
	}

	d.bucketName = uriParts[0]
	if len(uriParts) > 1 {
		d.prefix = strings.Join(uriParts[1:], "/")
	}

	client, err := storage.NewClient(d.ctx, option.WithScopes(storage.ScopeReadWrite))
	if err != nil {
		return err
	}

	d.client = client

	_, err = client.Bucket(d.bucketName).Attrs(ctx)
	return err
}

// StartUpload will begin the GCS upload workers
func (d *GoogleCloudStorageBackend) StartUpload(in <-chan *helpers.VolumeInfo) <-chan *helpers.VolumeInfo {
	out := make(chan *helpers.VolumeInfo)
	d.wg.Add(d.conf.MaxParallelUploads)
	for i := 0; i < d.conf.MaxParallelUploads; i++ {
		go func() {
			uploader(d.ctx, d.uploadWrapper, "gs", d.conf.getExpBackoff(), in, out)
			d.wg.Done()
		}()
	}

	go func() {
		d.Wait()
		close(out)
	}()

	return out
}

func (d *GoogleCloudStorageBackend) uploadWrapper(vol *helpers.VolumeInfo) func() error {
	// TODO: Enable resumeable uploads
	return func() error {
		select {
		case <-d.ctx.Done():
			return nil
		default:
			d.conf.MaxParallelUploadBuffer <- true
			defer func() {
				<-d.conf.MaxParallelUploadBuffer
			}()

			if err := vol.OpenVolume(); err != nil {
				helpers.AppLogger.Debugf("gs backend: Error while opening volume %s - %v", vol.ObjectName, err)
				return err
			}
			defer vol.Close()
			objName := d.prefix + vol.ObjectName
			w := d.client.Bucket(d.bucketName).Object(objName).NewWriter(d.ctx)
			defer w.Close()
			w.CRC32C = vol.CRC32CSum32
			_, err := io.Copy(w, vol)
			if err != nil {
				helpers.AppLogger.Debugf("gs backend: Error while uploading volume %s - %v", vol.ObjectName, err)
				// Check if the context was canceled, and if so, don't let the backoff function retry
				select {
				case <-d.ctx.Done():
					return nil
				default:
				}
			}
			return err
		}
	}
}

// Delete will delete the given object from the configured bucket
func (d *GoogleCloudStorageBackend) Delete(filename string) error {
	return d.client.Bucket(d.bucketName).Object(filename).Delete(d.ctx)
}

// PreDownload does nothing on this backend.
func (d *GoogleCloudStorageBackend) PreDownload(objects []string) error {
	return nil
}

// Get will download the requseted object
func (d *GoogleCloudStorageBackend) Get(filename string) (io.ReadCloser, error) {
	return d.client.Bucket(d.bucketName).Object(filename).NewReader(d.ctx)
}

// Wait will wait until all volumes have been processed from the incoming
// channel.
func (d *GoogleCloudStorageBackend) Wait() {
	d.wg.Wait()
}

// Close will signal the upload workers to stop processing the contents of the incoming channel
// and to close the outgoing channel. It will also cancel any ongoing requests.
func (d *GoogleCloudStorageBackend) Close() error {
	d.cancel()
	d.Wait()
	// Close the storage client as well
	if err := d.client.Close(); err != nil {
		return err
	}
	d.client = nil
	return nil
}

// List will iterate through all objects in the configured GCS bucket and return
// a list of object names.
func (d *GoogleCloudStorageBackend) List(prefix string) ([]string, error) {
	q := &storage.Query{Prefix: prefix}
	objects := d.client.Bucket(d.bucketName).Objects(d.ctx, q)
	l := make([]string, 0, 1000)
	for {
		attrs, err := objects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gs backend: could not list bucket due to error - %v", err)
		}

		l = append(l, attrs.Name)
	}
	return l, nil
}

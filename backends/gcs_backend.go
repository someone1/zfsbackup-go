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

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
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
	wg         *errgroup.Group
	client     clientInterface
	prefix     string
	bucketName string
}

// Maybe they'll figure out something better eventually...
// // https://code-review.googlesource.com/#/c/12970/
type clientInterface interface {
	BucketExists(context.Context, string) error
	DeleteObject(c context.Context, b, o string) error
	NewWriter(c context.Context, b, o string, h uint32) io.WriteCloser
	NewReader(c context.Context, b, o string) (io.ReadCloser, error)
	ListBucket(c context.Context, b, p string) ([]string, error)
	Close() error
}

// Basic wrapper for *storage.Client - will not be tested
type gcsClient struct {
	client *storage.Client
}

func (g *gcsClient) BucketExists(ctx context.Context, bucket string) error {
	_, err := g.client.Bucket(bucket).Attrs(ctx)
	return err
}

func (g *gcsClient) DeleteObject(ctx context.Context, bucket, object string) error {
	return g.client.Bucket(bucket).Object(object).Delete(ctx)
}

func (g *gcsClient) NewWriter(ctx context.Context, bucket, object string, crc32Hash uint32) io.WriteCloser {
	w := g.client.Bucket(bucket).Object(object).NewWriter(ctx)
	w.CRC32C = crc32Hash
	w.SendCRC32C = true
	return w
}

func (g *gcsClient) NewReader(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	return g.client.Bucket(bucket).Object(object).NewReader(ctx)
}

func (g *gcsClient) Close() error {
	return g.client.Close()
}

func (g *gcsClient) ListBucket(ctx context.Context, bucket, prefix string) ([]string, error) {
	q := &storage.Query{Prefix: prefix}
	objects := g.client.Bucket(bucket).Objects(ctx, q)
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

// Init will initialize the GoogleCloudStorageBackend and verify the provided URI is valid/exists.
func (d *GoogleCloudStorageBackend) Init(ctx context.Context, conf *BackendConfig) error {
	client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadWrite))
	if err != nil {
		return err
	}

	return d.initWithClient(ctx, conf, &gcsClient{client})
}

func (d *GoogleCloudStorageBackend) initWithClient(ctx context.Context, conf *BackendConfig, client clientInterface) error {
	d.conf = conf

	cleanPrefix := strings.TrimPrefix(d.conf.TargetURI, "gs://")
	if cleanPrefix == d.conf.TargetURI {
		return ErrInvalidURI
	}

	uriParts := strings.Split(cleanPrefix, "/")

	d.bucketName = uriParts[0]
	if len(uriParts) > 1 {
		d.prefix = strings.Join(uriParts[1:], "/")
	}

	d.client = client
	return d.client.BucketExists(ctx, d.bucketName)
}

// StartUpload will begin the GCS upload workers
func (d *GoogleCloudStorageBackend) StartUpload(ctx context.Context, in <-chan *helpers.VolumeInfo) <-chan *helpers.VolumeInfo {
	out, wgw := retryUploadOrchestrator(ctx, in, d.uploadWrapper, d.conf, d.conf.MaxParallelUploads)
	d.wg = wgw
	return out
}

func (d *GoogleCloudStorageBackend) uploadWrapper(ctx context.Context, vol *helpers.VolumeInfo) func() error {
	return func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
			w := d.client.NewWriter(ctx, d.bucketName, objName, vol.CRC32CSum32)
			defer w.Close()
			_, err := io.Copy(w, vol)
			if err != nil {
				helpers.AppLogger.Debugf("gs backend: Error while uploading volume %s - %v", vol.ObjectName, err)
			}
			return err
		}
	}
}

// Delete will delete the given object from the configured bucket
func (d *GoogleCloudStorageBackend) Delete(ctx context.Context, filename string) error {
	return d.client.DeleteObject(ctx, d.bucketName, filename)
}

// PreDownload does nothing on this backend.
func (d *GoogleCloudStorageBackend) PreDownload(ctx context.Context, objects []string) error {
	return nil
}

// Get will download the requseted object
func (d *GoogleCloudStorageBackend) Get(ctx context.Context, filename string) (io.ReadCloser, error) {
	return d.client.NewReader(ctx, d.bucketName, filename)
}

// Wait will wait until all volumes have been processed from the incoming
// channel.
func (d *GoogleCloudStorageBackend) Wait() error {
	if d.wg != nil {
		return d.wg.Wait()
	}
	return nil
}

// Close will wait for any ongoing operations to complete then close and release any resources used by the GCS backend.
func (d *GoogleCloudStorageBackend) Close() error {
	_ = d.Wait()

	// Close the storage client as well
	err := d.client.Close()
	d.client = nil
	return err
}

// List will iterate through all objects in the configured GCS bucket and return
// a list of object names.
func (d *GoogleCloudStorageBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return d.client.ListBucket(ctx, d.bucketName, prefix)
}

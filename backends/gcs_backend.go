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
	client     GCSClientInterface
	prefix     string
	bucketName string
}

// GCSClientInterface is used to abstract the underlysing GCS client
// so we can mockup tests.
// Maybe they'll figure out something better eventually...
// // https://code-review.googlesource.com/#/c/12970/
type GCSClientInterface interface {
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

type withGCSClient struct{ client GCSClientInterface }

func (w withGCSClient) Apply(b Backend) {
	switch v := b.(type) {
	case *GoogleCloudStorageBackend:
		v.client = w.client
	}
}

// WithGCSClient will override an GCS backend's underlying API client with the one provided.
// Primarily used to inject mock clients for testing.
func WithGCSClient(c GCSClientInterface) Option {
	return withGCSClient{c}
}

// Init will initialize the GoogleCloudStorageBackend and verify the provided URI is valid/exists.
func (g *GoogleCloudStorageBackend) Init(ctx context.Context, conf *BackendConfig, opts ...Option) error {
	g.conf = conf

	cleanPrefix := strings.TrimPrefix(g.conf.TargetURI, "gs://")
	if cleanPrefix == g.conf.TargetURI {
		return ErrInvalidURI
	}

	uriParts := strings.Split(cleanPrefix, "/")

	g.bucketName = uriParts[0]
	if len(uriParts) > 1 {
		g.prefix = strings.Join(uriParts[1:], "/")
	}

	for _, opt := range opts {
		opt.Apply(g)
	}

	if g.client == nil {
		client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadWrite))
		if err != nil {
			return err
		}
		g.client = &gcsClient{client}
	}

	return g.client.BucketExists(ctx, g.bucketName)
}

// Upload will upload the provided VolumeInfo to Google's Cloud Storage
func (g *GoogleCloudStorageBackend) Upload(ctx context.Context, vol *helpers.VolumeInfo) error {
	g.conf.MaxParallelUploadBuffer <- true
	defer func() {
		<-g.conf.MaxParallelUploadBuffer
	}()

	objName := g.prefix + vol.ObjectName
	w := g.client.NewWriter(ctx, g.bucketName, objName, vol.CRC32CSum32)
	defer w.Close()
	_, err := io.Copy(w, vol)
	if err != nil {
		helpers.AppLogger.Debugf("gs backend: Error while uploading volume %s - %v", vol.ObjectName, err)
	}
	return err

}

// Delete will delete the given object from the configured bucket
func (g *GoogleCloudStorageBackend) Delete(ctx context.Context, filename string) error {
	return g.client.DeleteObject(ctx, g.bucketName, filename)
}

// PreDownload does nothing on this backend.
func (g *GoogleCloudStorageBackend) PreDownload(ctx context.Context, objects []string) error {
	return nil
}

// Download will download the requseted object which can be read from the return io.ReadCloser.
func (g *GoogleCloudStorageBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	return g.client.NewReader(ctx, g.bucketName, filename)
}

// Close will release any resources used by the GCS backend.
func (g *GoogleCloudStorageBackend) Close() error {
	// Close the storage client as well
	err := g.client.Close()
	g.client = nil
	return err
}

// List will iterate through all objects in the configured GCS bucket and return
// a list of object names, filtering by the prefix provided.
func (g *GoogleCloudStorageBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return g.client.ListBucket(ctx, g.bucketName, prefix)
}

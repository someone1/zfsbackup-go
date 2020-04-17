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

	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
)

// GoogleCloudStorageBackendPrefix is the URI prefix used for the GoogleCloudStorageBackend.
const GoogleCloudStorageBackendPrefix = "gs"

// Authenticate: https://developers.google.com/identity/protocols/application-default-credentials

// GoogleCloudStorageBackend integrates with Google Cloud Storage.
type GoogleCloudStorageBackend struct {
	conf       *BackendConfig
	client     *storage.Client
	prefix     string
	bucketName string
}

type withGoogleCloudStorageClient struct{ client *storage.Client }

func (w withGoogleCloudStorageClient) Apply(b Backend) {
	if v, ok := b.(*GoogleCloudStorageBackend); ok {
		v.client = w.client
	}
}

// WithGoogleCloudStorageClient will override an GCS backend's underlying API client with the one provided.
func WithGoogleCloudStorageClient(c *storage.Client) Option {
	return withGoogleCloudStorageClient{c}
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
		g.client = client
	}

	if _, err := g.client.Bucket(g.bucketName).Attrs(ctx); err != nil {
		return err
	}

	return nil
}

// Upload will upload the provided VolumeInfo to Google's Cloud Storage
// It utilizes Resumeable Uploads to upload one file at a time serially.
// Incomplete uploads auto-expire after one week. See:
// https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
func (g *GoogleCloudStorageBackend) Upload(ctx context.Context, vol *files.VolumeInfo) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g.conf.MaxParallelUploadBuffer <- true
	defer func() {
		<-g.conf.MaxParallelUploadBuffer
	}()

	objName := g.prefix + vol.ObjectName
	w := g.client.Bucket(g.bucketName).Object(objName).NewWriter(ctx)
	w.CRC32C = vol.CRC32CSum32
	w.SendCRC32C = true
	w.ChunkSize = g.conf.UploadChunkSize

	if _, err := io.Copy(w, vol); err != nil {
		log.AppLogger.Debugf("gs backend: Error while uploading volume %s - %v", vol.ObjectName, err)
		return err
	}
	return w.Close()
}

// Delete will delete the given object from the configured bucket
func (g *GoogleCloudStorageBackend) Delete(ctx context.Context, filename string) error {
	return g.client.Bucket(g.bucketName).Object(g.prefix + filename).Delete(ctx)
}

// PreDownload does nothing on this backend.
func (g *GoogleCloudStorageBackend) PreDownload(ctx context.Context, objects []string) error {
	return nil
}

// Download will download the requseted object which can be read from the return io.ReadCloser.
func (g *GoogleCloudStorageBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	return g.client.Bucket(g.bucketName).Object(g.prefix + filename).NewReader(ctx)
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
	q := &storage.Query{Prefix: g.prefix + prefix}
	objects := g.client.Bucket(g.bucketName).Objects(ctx, q)
	l := make([]string, 0, 1000)
	for {
		attrs, err := objects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gs backend: could not list bucket due to error - %v", err)
		}

		l = append(l, strings.TrimPrefix(attrs.Name, g.prefix))
	}
	return l, nil
}

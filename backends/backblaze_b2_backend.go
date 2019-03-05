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
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/kurin/blazer/b2"

	"github.com/someone1/zfsbackup-go/helpers"
)

// B2BackendPrefix is the URI prefix used for the B2Backend.
const B2BackendPrefix = "b2"

// B2Backend integrates with BackBlaze's B2 storage service.
type B2Backend struct {
	conf       *BackendConfig
	bucketCli  *b2.Bucket
	mutex      sync.Mutex
	prefix     string
	bucketName string
}

type bufferedRT struct {
	bufChan chan bool
}

func (b bufferedRT) RoundTrip(r *http.Request) (*http.Response, error) {
	b.bufChan <- true
	defer func() { <-b.bufChan }()
	return http.DefaultTransport.RoundTrip(r)
}

// Init will initialize the B2Backend and verify the provided URI is valid/exists.
func (b *B2Backend) Init(ctx context.Context, conf *BackendConfig, opts ...Option) error {
	b.conf = conf

	cleanPrefix := strings.TrimPrefix(b.conf.TargetURI, B2BackendPrefix+"://")
	if cleanPrefix == b.conf.TargetURI {
		return ErrInvalidURI
	}

	accountID := os.Getenv("B2_ACCOUNT_ID")
	accountKey := os.Getenv("B2_ACCOUNT_KEY")

	uriParts := strings.Split(cleanPrefix, "/")

	b.bucketName = uriParts[0]
	if len(uriParts) > 1 {
		b.prefix = strings.Join(uriParts[1:], "/")
	}

	for _, opt := range opts {
		opt.Apply(b)
	}

	var cliopts []b2.ClientOption
	if conf.MaxParallelUploadBuffer != nil {
		cliopts = append(cliopts, b2.Transport(bufferedRT{b.conf.MaxParallelUploadBuffer}))
	}

	client, err := b2.NewClient(ctx, accountID, accountKey, cliopts...)
	if err != nil {
		return err
	}

	b.bucketCli, err = client.Bucket(ctx, b.bucketName)
	if err != nil {
		return err
	}

	// Poke the bucket to ensure it exists.
	iter := b.bucketCli.List(ctx, b2.ListPageSize(1))
	iter.Next()
	return iter.Err()
}

// Upload will upload the provided volume to this B2Backend's configured bucket+prefix
func (b *B2Backend) Upload(ctx context.Context, vol *helpers.VolumeInfo) error {
	// We will be doing multipart uploads, no need to allow multiple calls of Upload to initiate new uploads.
	b.mutex.Lock()
	defer b.mutex.Unlock()

	name := b.prefix + vol.ObjectName
	w := b.bucketCli.Object(name).NewWriter(ctx)

	w.ConcurrentUploads = b.conf.MaxParallelUploads
	w.ChunkSize = b.conf.UploadChunkSize
	w.Resume = true

	sha1Opt := b2.WithAttrsOption(&b2.Attrs{SHA1: vol.SHA1Sum})
	sha1Opt(w)

	if _, err := io.Copy(w, vol); err != nil {
		w.Close()
		helpers.AppLogger.Debugf("b2 backend: Error while uploading volume %s - %v", vol.ObjectName, err)
		return err
	}

	return w.Close()
}

// Delete will delete the object with the given name from the configured bucket
func (b *B2Backend) Delete(ctx context.Context, name string) error {
	return b.bucketCli.Object(name).Delete(ctx)
}

// PreDownload will do nothing for this backend.
func (b *B2Backend) PreDownload(ctx context.Context, keys []string) error {
	return nil
}

// Download will download the requseted object which can be read from the returned io.ReadCloser
func (b *B2Backend) Download(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.bucketCli.Object(name).NewReader(ctx), nil
}

// Close will release any resources used by the B2 backend.
func (b *B2Backend) Close() error {
	b.bucketCli = nil
	return nil
}

// List will iterate through all objects in the configured B2 bucket and return
// a list of object names, filtering by the provided prefix.
func (b *B2Backend) List(ctx context.Context, prefix string) ([]string, error) {
	var l []string
	iter := b.bucketCli.List(ctx, b2.ListPrefix(prefix))
	for iter.Next() {
		obj := iter.Object()
		l = append(l, obj.Name())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return l, nil
}

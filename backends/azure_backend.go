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
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/storage"
	"golang.org/x/sync/errgroup"

	"github.com/someone1/zfsbackup-go/helpers"
)

// Keep an eye on the Azure Go SDK, apparently there's a rewrite in progress: https://github.com/Azure/azure-sdk-for-go/issues/626#issuecomment-324398278

// AzureBackendPrefix is the URI prefix used for the AzureBackend.
const AzureBackendPrefix = "azure"

// AzureBackend integrates with Microsoft's Azure Storage Services.
type AzureBackend struct {
	conf          *BackendConfig
	mutex         sync.Mutex
	accountName   string
	accountKey    string
	azureURL      string
	prefix        string
	containerName string
}

type contextSender struct {
	ctx context.Context
	s   storage.Sender
}

// Send will ensure the request given is tied to the context we want it to be tied to.
func (c *contextSender) Send(client *storage.Client, req *http.Request) (*http.Response, error) {
	r := req.WithContext(c.ctx)
	return c.s.Send(client, r)
}

// We need to create a new client for every API interaction as the Azure SDK doesn't support contexts!
func (a *AzureBackend) getContainerClient(ctx context.Context) (*storage.Container, error) {
	client, err := storage.NewClient(a.accountName, a.accountKey, a.azureURL, storage.DefaultAPIVersion, a.accountName != storage.StorageEmulatorAccountName)
	if err != nil {
		return nil, err
	}
	client.Sender = &contextSender{
		ctx: ctx,
		s:   client.Sender,
	}
	blobCli := client.GetBlobService()
	return blobCli.GetContainerReference(a.containerName), nil
}

// Init will initialize the AzureBackend and verify the provided URI is valid/exists.
func (a *AzureBackend) Init(ctx context.Context, conf *BackendConfig, opts ...Option) error {
	a.conf = conf

	cleanPrefix := strings.TrimPrefix(a.conf.TargetURI, AzureBackendPrefix+"://")
	if cleanPrefix == a.conf.TargetURI {
		return ErrInvalidURI
	}

	a.accountName = os.Getenv("AZURE_ACCOUNT_NAME")
	a.accountKey = os.Getenv("AZURE_ACCOUNT_KEY")
	a.azureURL = os.Getenv("AZURE_CUSTOM_ENDPOINT")
	if a.azureURL == "" {
		a.azureURL = storage.DefaultBaseURL
	}

	uriParts := strings.Split(cleanPrefix, "/")

	a.containerName = uriParts[0]
	if len(uriParts) > 1 {
		a.prefix = strings.Join(uriParts[1:], "/")
	}

	for _, opt := range opts {
		opt.Apply(a)
	}

	container, err := a.getContainerClient(ctx)
	if err != nil {
		return err
	}

	_, err = container.ListBlobs(storage.ListBlobsParameters{MaxResults: 0})
	return err
}

// Upload will upload the provided volume to this AzureBackend's configured container+prefix
func (a *AzureBackend) Upload(ctx context.Context, vol *helpers.VolumeInfo) error {
	// We will achieve parallel upload by splitting a single upload into chunks
	// so don't let multiple calls to this function run in parallel.
	a.mutex.Lock()
	defer a.mutex.Unlock()

	container, err := a.getContainerClient(ctx)
	if err != nil {
		return err
	}
	name := a.prefix + vol.ObjectName
	blob := container.GetBlobReference(name)

	// We will PutBlock for chunks of UploadChunkSize and then finalize the block with a PutBlockList call
	// Would use append, but append calls from the SDK don't support MD5 headers? https://github.com/Azure/azure-sdk-for-go/issues/757

	// First initialize an empty block blob
	err = blob.CreateBlockBlob(nil)
	if err != nil {
		return err
	}

	var (
		blocks    []storage.Block
		errg      errgroup.Group
		blockid   uint32
		readBytes uint64
	)
	bs := make([]byte, 4)

	// Currently, we can only have a max of 50000 blocks, 100MiB each, but we don't expect chunks that large
	// Upload the object in chunks
	for {
		binary.LittleEndian.PutUint32(bs, blockid)
		block := storage.Block{
			ID:     base64.StdEncoding.EncodeToString(bs),
			Status: storage.BlockStatusLatest,
		}
		blocks = append(blocks, block)
		blockid++

		blockSize := uint64(a.conf.UploadChunkSize)
		if !vol.IsUsingPipe() && blockSize > vol.Size-readBytes {
			blockSize = vol.Size - readBytes
		}

		buf := make([]byte, blockSize)
		n, rerr := io.ReadFull(vol, buf)
		if rerr != nil && rerr != io.ErrUnexpectedEOF {
			return rerr
		}

		readBytes += uint64(n)
		if n > 0 {
			md5sum := md5.Sum(buf[:n])

			select {
			case <-ctx.Done():
				return ctx.Err()
			case a.conf.MaxParallelUploadBuffer <- true:
				errg.Go(func() error {
					defer func() { <-a.conf.MaxParallelUploadBuffer }()
					return blob.PutBlockWithLength(block.ID, uint64(n), bytes.NewBuffer(buf[:n]), &storage.PutBlockOptions{
						ContentMD5: base64.StdEncoding.EncodeToString(md5sum[:]),
					})
				})
			}
		}

		if !vol.IsUsingPipe() && readBytes == vol.Size || rerr == io.ErrUnexpectedEOF {
			break
		}
	}

	err = errg.Wait()
	if err != nil {
		helpers.AppLogger.Debugf("azure backend: Error while uploading volume %s - %v", vol.ObjectName, err)
		return err
	}

	md5Raw, merr := hex.DecodeString(vol.MD5Sum)
	if merr != nil {
		return merr
	}
	blob.Properties.ContentMD5 = base64.StdEncoding.EncodeToString(md5Raw)

	// Finally, finalize the storage blob by giving Azure the block list order
	err = blob.PutBlockList(blocks, nil)
	if err != nil {
		helpers.AppLogger.Debugf("azure backend: Error while finalizing volume %s - %v", vol.ObjectName, err)
	}
	return err
}

// Delete will delete the given object from the configured container
func (a *AzureBackend) Delete(ctx context.Context, name string) error {
	container, err := a.getContainerClient(ctx)
	if err != nil {
		return err
	}
	blob := container.GetBlobReference(name)
	return blob.Delete(nil)
}

// PreDownload will do nothing for this backend.
func (a *AzureBackend) PreDownload(ctx context.Context, keys []string) error {
	return nil
}

// Download will download the requseted object which can be read from the returned io.ReadCloser
func (a *AzureBackend) Download(ctx context.Context, name string) (io.ReadCloser, error) {
	container, err := a.getContainerClient(ctx)
	if err != nil {
		return nil, err
	}
	blob := container.GetBlobReference(name)
	return blob.Get(nil)
}

// Close will release any resources used by the Azure backend.
func (a *AzureBackend) Close() error {
	return nil
}

// List will iterate through all objects in the configured Azure Storage Container and return
// a list of blob names, filtering by the provided prefix.
func (a *AzureBackend) List(ctx context.Context, prefix string) ([]string, error) {
	container, err := a.getContainerClient(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := container.ListBlobs(storage.ListBlobsParameters{
		MaxResults: 5000,
		Prefix:     prefix,
	})

	if err != nil {
		return nil, err
	}

	l := make([]string, 0, len(resp.Blobs))
	for {
		for _, obj := range resp.Blobs {
			l = append(l, obj.Name)
		}

		if resp.NextMarker == "" {
			break
		}

		resp, err = container.ListBlobs(storage.ListBlobsParameters{
			MaxResults: 5000,
			Prefix:     prefix,
			Marker:     resp.NextMarker,
		})

		if err != nil {
			return nil, err
		}
	}

	return l, nil
}

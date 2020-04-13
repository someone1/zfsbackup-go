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
	"crypto/md5" // nolint:gosec // MD5 not used for crytopgrahic purposes
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
)

// AzureBackendPrefix is the URI prefix used for the AzureBackend.
const (
	AzureBackendPrefix = "azure"
	blobAPIURL         = "blob.core.windows.net"
)

var (
	errContainerMismatch = errors.New("container name in SAS URI is different than destination container provided")
)

// AzureBackend integrates with Microsoft's Azure Storage Services.
type AzureBackend struct {
	conf          *BackendConfig
	mutex         sync.Mutex
	accountName   string
	accountKey    string
	containersas  string
	azureURL      string
	prefix        string
	containerName string
	containerSvc  azblob.ContainerURL
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
	a.containersas = os.Getenv("AZURE_SAS_URI")
	a.azureURL = os.Getenv("AZURE_CUSTOM_ENDPOINT")
	if a.azureURL == "" {
		a.azureURL = fmt.Sprintf("https://%s.%s", a.accountName, blobAPIURL)
	}

	uriParts := strings.Split(cleanPrefix, "/")

	a.containerName = uriParts[0]
	if len(uriParts) > 1 {
		a.prefix = strings.Join(uriParts[1:], "/")
	}

	for _, opt := range opts {
		opt.Apply(a)
	}

	// No need for this logging
	pipelineOpts := azblob.PipelineOptions{
		RequestLog: azblob.RequestLogOptions{
			LogWarningIfTryOverThreshold: -1,
		},
	}
	if a.containersas != "" {
		parsedsas, err := url.Parse(a.containersas)
		if err != nil {
			return errors.Wrap(err, "failed to parse SAS URI")
		}
		pipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), pipelineOpts)
		sasParts := azblob.NewBlobURLParts(*parsedsas)
		if sasParts.ContainerName != a.containerName {
			return errContainerMismatch
		}
		a.containerSvc = azblob.NewContainerURL(*parsedsas, pipeline)
	} else {
		credential, err := azblob.NewSharedKeyCredential(a.accountName, a.accountKey)
		if err != nil {
			return errors.Wrap(err, "failed to initilze SAS credential")
		}
		destURL, err := url.Parse(a.azureURL)
		if err != nil {
			return errors.Wrap(err, "failed to construct Azure API URL")
		}
		pipeline := azblob.NewPipeline(credential, pipelineOpts)
		svcURL := azblob.NewServiceURL(*destURL, pipeline)
		a.containerSvc = svcURL.NewContainerURL(a.containerName)
	}

	_, err := a.containerSvc.ListBlobsFlatSegment(ctx, azblob.Marker{}, azblob.ListBlobsSegmentOptions{MaxResults: 0})
	return err
}

// Upload will upload the provided volume to this AzureBackend's configured container+prefix
// It utilizes Azure Block Blob uploads to upload chunks of a single file concurrently. Partial
// uploads are automatically garbage collected after one week. See:
// https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
func (a *AzureBackend) Upload(ctx context.Context, vol *files.VolumeInfo) error {
	// We will achieve parallel upload by splitting a single upload into chunks
	// so don't let multiple calls to this function run in parallel.
	a.mutex.Lock()
	defer a.mutex.Unlock()

	name := a.prefix + vol.ObjectName
	blobURL := a.containerSvc.NewBlockBlobURL(name)

	// We will PutBlock for chunks of UploadChunkSize and then finalize the block with a PutBlockList call
	// Staging blocks does not allow MD5 checksums: https://github.com/Azure/azure-storage-blob-go/issues/56

	// https://godoc.org/github.com/Azure/azure-storage-blob-go/2018-03-28/azblob#NewBlockBlobURL
	// These helper functions convert a binary block ID to a base-64 string and vice versa
	// NOTE: The blockID must be <= 64 bytes and ALL blockIDs for the block must be the same length
	blockIDBinaryToBase64 := func(blockID []byte) string { return base64.StdEncoding.EncodeToString(blockID) }
	// These helper functions convert an int block ID to a base-64 string and vice versa
	blockIDIntToBase64 := func(blockID int) string {
		binaryBlockID := (&[4]byte{})[:] // All block IDs are 4 bytes long
		binary.LittleEndian.PutUint32(binaryBlockID, uint32(blockID))
		return blockIDBinaryToBase64(binaryBlockID)
	}

	var (
		blockIDs  []string
		errg      errgroup.Group
		blockid   int
		readBytes uint64
	)

	// Currently, we can only have a max of 50000 blocks, 100MiB each, but we don't expect chunks that large
	// Upload the object in chunks
	for {
		blockID := blockIDIntToBase64(blockid)
		blockIDs = append(blockIDs, blockID)
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
			// nolint:gosec // MD5 not used for crytopgraphic purposes
			md5sum := md5.Sum(buf[:n])

			select {
			case <-ctx.Done():
				return ctx.Err()
			case a.conf.MaxParallelUploadBuffer <- true:
				errg.Go(func() error {
					defer func() { <-a.conf.MaxParallelUploadBuffer }()
					_, err := blobURL.StageBlock(ctx, blockID, bytes.NewReader(buf[:n]), azblob.LeaseAccessConditions{}, md5sum[:])
					return err
				})
			}
		}

		if !vol.IsUsingPipe() && readBytes == vol.Size || rerr == io.ErrUnexpectedEOF {
			break
		}
	}

	err := errg.Wait()
	if err != nil {
		log.AppLogger.Debugf("azure backend: Error while uploading volume %s - %v", vol.ObjectName, err)
		return err
	}

	md5Raw, merr := hex.DecodeString(vol.MD5Sum)
	if merr != nil {
		return merr
	}

	// Finally, finalize the storage blob by giving Azure the block list order
	_, err = blobURL.CommitBlockList(ctx, blockIDs, azblob.BlobHTTPHeaders{ContentMD5: md5Raw}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		log.AppLogger.Debugf("azure backend: Error while finalizing volume %s - %v", vol.ObjectName, err)
	}
	return err
}

// Delete will delete the given object from the configured container
func (a *AzureBackend) Delete(ctx context.Context, name string) error {
	blobURL := a.containerSvc.NewBlobURL(name)
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	return err
}

// PreDownload will do nothing for this backend.
func (a *AzureBackend) PreDownload(ctx context.Context, keys []string) error {
	return nil
}

// Download will download the requseted object which can be read from the returned io.ReadCloser
func (a *AzureBackend) Download(ctx context.Context, name string) (io.ReadCloser, error) {
	blobURL := a.containerSvc.NewBlobURL(name)
	resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}
	return resp.Body(azblob.RetryReaderOptions{}), nil
}

// Close will release any resources used by the Azure backend.
func (a *AzureBackend) Close() error {
	return nil
}

// List will iterate through all objects in the configured Azure Storage Container and return
// a list of blob names, filtering by the provided prefix.
func (a *AzureBackend) List(ctx context.Context, prefix string) ([]string, error) {
	l := make([]string, 0, 5000)

	for marker := (azblob.Marker{}); marker.NotDone(); {
		resp, err := a.containerSvc.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
			Prefix:     prefix,
			MaxResults: 5000,
		})
		if err != nil {
			return nil, errors.Wrap(err, "error while listing blobs from container")
		}

		for _, obj := range resp.Segment.BlobItems {
			l = append(l, obj.Name)
		}

		marker = resp.NextMarker
	}

	return l, nil
}

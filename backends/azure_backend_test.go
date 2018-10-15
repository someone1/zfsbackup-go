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
	"io"
	"net/url"
	"os"
	"reflect"
	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

func TestAzureGetBackendForURI(t *testing.T) {
	b, err := GetBackendForURI(AzureBackendPrefix + "://bucket_name")
	if err != nil {
		t.Errorf("Error while trying to get backend: %v", err)
	}
	if _, ok := b.(*AzureBackend); !ok {
		t.Errorf("Expected to get a backend of type AzureBackend, but did not.")
	}
}

const (
	azureTestBucketName = "azuretestbucket"
	testSASURI          = "https://storageaccount.blob.core.windows.net/azuretestbucket"
)

func TestAzureBackend(t *testing.T) {
	if os.Getenv("AZURE_CUSTOM_ENDPOINT") == "" {
		t.Skip("No custom Azure Endpoint provided to test against")
	}
	err := os.Setenv("AZURE_ACCOUNT_NAME", storage.StorageEmulatorAccountName)
	if err != nil {
		t.Fatalf("could not set environmental variable due to error: %v", err)
	}
	err = os.Setenv("AZURE_ACCOUNT_KEY", storage.StorageEmulatorAccountKey)
	if err != nil {
		t.Fatalf("could not set environmental variable due to error: %v", err)
	}

	b, err := GetBackendForURI(AzureBackendPrefix + "://bucket_name")
	if err != nil {
		t.Fatalf("Error while trying to get backend: %v", err)
	}

	ctx := context.Background()

	credential, err := azblob.NewSharedKeyCredential(storage.StorageEmulatorAccountName, storage.StorageEmulatorAccountKey)
	if err != nil {
		t.Fatalf("failed to parse SAS key: %v", err)
	}
	destURL, err := url.Parse(os.Getenv("AZURE_CUSTOM_ENDPOINT"))
	if err != nil {
		t.Fatalf("failed to construct Azure API URL: %v", err)
	}
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	svcURL := azblob.NewServiceURL(*destURL, pipeline)
	containerSvc := svcURL.NewContainerURL(azureTestBucketName)
	if _, err = containerSvc.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone); err != nil {
		t.Fatalf("error while creating bucket: %v", err)
	}

	defer containerSvc.Delete(ctx, azblob.ContainerAccessConditions{})

	testPayLoad, goodVol, badVol, perr := prepareTestVols()
	if perr != nil {
		t.Fatalf("Error while creating test volumes: %v", perr)
	}
	defer goodVol.DeleteVolume()
	defer badVol.DeleteVolume()

	t.Run("Init", func(t *testing.T) {
		// Bad TargetURI
		conf := &BackendConfig{
			TargetURI:               "notvalid://" + azureTestBucketName,
			UploadChunkSize:         8 * 1024 * 1024,
			MaxParallelUploads:      5,
			MaxParallelUploadBuffer: make(chan bool, 5),
		}
		err := b.Init(ctx, conf)
		if err != ErrInvalidURI {
			t.Fatalf("Issue initilazing AzureBackend: %v", err)
		}

		// Good TargetURI
		conf = &BackendConfig{
			TargetURI:               AzureBackendPrefix + "://" + azureTestBucketName,
			UploadChunkSize:         8 * 1024 * 1024,
			MaxParallelUploads:      5,
			MaxParallelUploadBuffer: make(chan bool, 5),
		}
		err = b.Init(ctx, conf)
		if err != nil {
			t.Fatalf("Issue initilazing AzureBackend: %v", err)
		}
	})

	t.Run("Upload", func(t *testing.T) {
		err := goodVol.OpenVolume()
		if err != nil {
			t.Errorf("could not open good volume due to error %v", err)
		}
		defer goodVol.Close()
		err = b.Upload(ctx, goodVol)
		if err != nil {
			t.Fatalf("Issue uploading goodvol: %v", err)
		}

		// err = b.Upload(ctx, badVol)
		// if err == nil {
		// 	t.Fatalf("Expecting non-nil error, got nil instead.")
		// }
	})

	t.Run("List", func(t *testing.T) {
		names, err := b.List(ctx, "")
		if err != nil {
			t.Fatalf("Issue listing container: %v\n%v", err, names)
		}

		if len(names) != 1 {
			t.Fatalf("Expecting exactly one name from list, got %d instead.", len(names))
		}

		if names[0] != goodVol.ObjectName {
			t.Fatalf("Expecting name '%s', got '%s' instead", goodVol.ObjectName, names[0])
		}

		names, err = b.List(ctx, "badprefix")
		if err != nil {
			t.Fatalf("Issue listing container: %v", err)
		}

		if len(names) != 0 {
			t.Fatalf("Expecting exactly zero names from list, got %d instead.", len(names))
		}
	})

	t.Run("PreDownload", func(t *testing.T) {
		err := b.PreDownload(ctx, nil)
		if err != nil {
			t.Fatalf("Issue calling PreDownload on AzureBackend: %v", err)
		}
	})

	t.Run("Download", func(t *testing.T) {
		r, err := b.Download(ctx, goodVol.ObjectName)
		if err != nil {
			t.Fatalf("Issue calling Download on AzureBackend: %v", err)
		}
		defer r.Close()

		buf := bytes.NewBuffer(nil)
		_, err = io.Copy(buf, r)
		if err != nil {
			t.Fatalf("error reading: %v", err)
		}

		if !reflect.DeepEqual(testPayLoad, buf.Bytes()) {
			t.Fatalf("downloaded object does not equal expected payload")
		}

		_, err = b.Download(ctx, badVol.ObjectName)
		if err == nil {
			t.Fatalf("expecting non-nil response, got nil instead")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		err := b.Delete(ctx, goodVol.ObjectName)
		if err != nil {
			t.Fatalf("Issue calling Delete on AzureBackend: %v", err)
		}

		names, err := b.List(ctx, "")
		if err != nil {
			t.Fatalf("Issue listing container: %v", err)
		}

		if len(names) != 0 {
			t.Fatalf("Expecting exactly zero names from list, got %d instead.", len(names))
		}
	})

	t.Run("Close", func(t *testing.T) {
		err := b.Close()
		if err != nil {
			t.Fatalf("Issue closing AzureBackend: %v", err)
		}
	})

	t.Run("SASURI", func(t *testing.T) {
		// We can't test SAS URI's against azurite since it's not yet supported, but we can at least test
		// that it parses it out correctly...
		// https://github.com/Azure/Azurite/issues/8

		var err error
		if err = os.Setenv("AZURE_SAS_URI", testSASURI); err != nil {
			t.Fatalf("could not set environmental variable due to error: %v", err)
		}

		// Different Container name
		conf := &BackendConfig{
			TargetURI:               AzureBackendPrefix + "://" + azureTestBucketName + "bad",
			UploadChunkSize:         8 * 1024 * 1024,
			MaxParallelUploads:      5,
			MaxParallelUploadBuffer: make(chan bool, 5),
		}
		err = b.Init(ctx, conf)
		if err != errContainerMismatch {
			t.Errorf("Expected container mismatch error initilazing AzureBackend w/ SAS URI, got %v", err)
		}

		// Should recieve a ResourceNotFound error if we actually try a valid init
		conf = &BackendConfig{
			TargetURI:               AzureBackendPrefix + "://" + azureTestBucketName,
			UploadChunkSize:         8 * 1024 * 1024,
			MaxParallelUploads:      5,
			MaxParallelUploadBuffer: make(chan bool, 5),
		}
		err = b.Init(ctx, conf)
		if err != nil {
			if serr, ok := err.(azblob.StorageError); ok {
				if serr.ServiceCode() != azblob.ServiceCodeResourceNotFound {
					t.Errorf("Expected `ResourceNotFound` error but got `%v` instead", serr.ServiceCode())
				}
			} else {
				t.Errorf("Expected a azblob.StorageError but got something else: %v", err)
			}
		} else {
			t.Fatalf("Expected non-nil error initilazing AzureBackend w/ SAS URI")
		}

		// Get rid of the env variable
		if err = os.Unsetenv("AZURE_SAS_URI"); err != nil {
			t.Fatalf("could not unset environmental variable due to error: %v", err)
		}
	})
}

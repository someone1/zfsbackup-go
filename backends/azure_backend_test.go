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
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
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
)

func TestAzureBackend(t *testing.T) {
	t.Parallel()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	defer func() {
		if _, err = containerSvc.Delete(ctx, azblob.ContainerAccessConditions{}); err != nil {
			t.Logf("could not delete container: %v", err)
		}
	}()

	b, err := GetBackendForURI(AzureBackendPrefix + "://bucket_name")
	if err != nil {
		t.Fatalf("Error while trying to get backend: %v", err)
	}

	BackendTest(ctx, AzureBackendPrefix, azureTestBucketName, false, b)(t)

	if err = os.Unsetenv("AZURE_ACCOUNT_KEY"); err != nil {
		t.Fatalf("could not unset env var - %v", err)
	}
	if err = os.Unsetenv("AZURE_ACCOUNT_NAME"); err != nil {
		t.Fatalf("could not unset env var - %v", err)
	}

	t.Run("SASURI", func(t *testing.T) {
		b, err = GetBackendForURI(AzureBackendPrefix + "://bucket_name")
		if err != nil {
			t.Fatalf("Error while trying to get backend: %v", err)
		}

		sasURI := generateSASURI(t, containerSvc.String(), azureTestBucketName, storage.StorageEmulatorAccountName, storage.StorageEmulatorAccountKey, 3600)

		if err = os.Setenv("AZURE_SAS_URI", sasURI); err != nil {
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

		BackendTest(ctx, AzureBackendPrefix, azureTestBucketName, true, b)(t)

		// Get rid of the env variable
		if err = os.Unsetenv("AZURE_SAS_URI"); err != nil {
			t.Fatalf("could not unset environmental variable due to error: %v", err)
		}
	})
}

func generateSASURI(t *testing.T, svcURL, container, accountName, key string, expireSecs int) string {
	t.Helper()

	perms := azblob.ContainerSASPermissions{Add: true, Create: true, Delete: true, List: true, Read: true, Write: true}
	sigValues := azblob.BlobSASSignatureValues{
		Version:       "2019-07-07",
		Protocol:      azblob.SASProtocolHTTPSandHTTP,
		Permissions:   perms.String(),
		ExpiryTime:    time.Now().Add(time.Second * time.Duration(expireSecs)).UTC(),
		ContainerName: container,
	}
	creds, err := azblob.NewSharedKeyCredential(accountName, key)
	if err != nil {
		t.Fatalf("could not generate creds: %v", err)
	}

	params, err := sigValues.NewSASQueryParameters(creds)
	if err != nil {
		t.Fatalf("could not generate params: %v", err)
	}

	u, _ := url.Parse(svcURL)
	u.RawQuery = params.Encode()

	return u.String()
}

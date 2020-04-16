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
	"crypto/tls"
	"net/http"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func TestGCSBackend(t *testing.T) {
	t.Parallel()

	if os.Getenv("GCS_FAKE_SERVER") == "" {
		t.Skip("No test GCS Endpoint provided to test against")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gcsTestBucketName := "test_bucket"

	// Setup Test Bucket
	transCfg := &http.Transport{
		// nolint:gosec // Purposely connecting to a local self-signed certificate endpoint
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	httpClient := &http.Client{Transport: transCfg}
	client, err := storage.NewClient(ctx, option.WithEndpoint(os.Getenv("GCS_FAKE_SERVER")+"/storage/v1/"), option.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatal(err)
	}

	if err = client.Bucket(gcsTestBucketName).Create(ctx, "test-project", nil); err != nil {
		t.Fatalf("could not create bucket; %v", err)
	}

	defer func() {
		// nolint:gocritic // Re-enable if this gets resolved: https://github.com/fsouza/fake-gcs-server/issues/214
		// it := testBucket.Objects(ctx, nil)
		// for {
		// 	attrs, iErr := it.Next()
		// 	if iErr == iterator.Done {
		// 		break
		// 	}
		// 	if iErr != nil {
		// 		t.Fatalf("could not list objects: %v", iErr)
		// 	}
		// 	if err = testBucket.Object(attrs.Name).Delete(ctx); err != nil {
		// 		t.Errorf("could not delete object: %v", err)
		// 	}
		// }
		// if err = client.Bucket(gcsTestBucketName).Delete(ctx); err != nil {
		// 	t.Errorf("could not delete bucket: %v", err)
		// }
	}()

	b, err := GetBackendForURI(GoogleCloudStorageBackendPrefix + "://bucket_name")
	if err != nil {
		t.Fatalf("Error while trying to get backend: %v", err)
	}

	BackendTest(ctx, GoogleCloudStorageBackendPrefix, gcsTestBucketName, false, b, WithGoogleCloudStorageClient(client))(t)
}

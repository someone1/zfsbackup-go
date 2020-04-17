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
	"os"
	"testing"

	"github.com/kurin/blazer/b2"
)

func TestB2GetBackendForURI(t *testing.T) {
	b, err := GetBackendForURI(B2BackendPrefix + "://bucket_name")
	if err != nil {
		t.Errorf("Error while trying to get backend: %v", err)
	}
	if _, ok := b.(*B2Backend); !ok {
		t.Errorf("Expected to get a backend of type B2Backend, but did not.")
	}
}

func TestB2Backend(t *testing.T) {
	t.Parallel()

	if os.Getenv("B2_ACCOUNT_ID") == "" {
		t.Skip("No test B2 credentials provided to test against")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b2TestBucketName := "zfsbackup-go"

	// Setup Test Bucket
	client, err := b2.NewClient(ctx, os.Getenv("B2_ACCOUNT_ID"), os.Getenv("B2_ACCOUNT_KEY"))
	if err != nil {
		t.Fatal(err)
	}

	bucketCli, err := client.NewBucket(ctx, b2TestBucketName, nil)
	if err != nil {
		t.Fatalf("could not create bucket; %v", err)
	}

	defer func() {
		it := bucketCli.List(ctx)
		for {
			if !it.Next() {
				break
			}

			if err = it.Object().Delete(ctx); err != nil {
				t.Errorf("could not delete object: %v", err)
			}
		}
		if it.Err() != nil {
			t.Errorf("could not list all files - %v", it.Err())
		}
		if err = bucketCli.Delete(ctx); err != nil {
			t.Errorf("could not delete bucket: %v", err)
		}
	}()

	b, err := GetBackendForURI(B2BackendPrefix + "://bucket_name")
	if err != nil {
		t.Fatalf("Error while trying to get backend: %v", err)
	}

	BackendTest(ctx, B2BackendPrefix, b2TestBucketName, false, b)(t)
}

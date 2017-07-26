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

// AWS S3 + 3rd Party S3 compatible destination integration test
// Expectation is that environment variables will be set properly to run tests with

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
)

type mockS3Client struct {
	s3iface.S3API
}

type mockS3Uploader struct {
	s3manageriface.UploaderAPI
}

var (
	s3BadBucket = "badbucket"
	s3BadKey    = "badkey"
)

func (m *mockS3Client) ListObjectsV2WithContext(ctx aws.Context, in *s3.ListObjectsV2Input, _ ...request.Option) (*s3.ListObjectsV2Output, error) {
	if *in.Bucket == s3BadBucket {
		return nil, errTest
	}

	return nil, nil
}

func (m *mockS3Client) DeleteObjectWithContext(ctx aws.Context, in *s3.DeleteObjectInput, _ ...request.Option) (*s3.DeleteObjectOutput, error) {
	if *in.Key == s3BadKey {
		return nil, errTest
	}

	return nil, nil
}

func (m *mockS3Client) GetObjectWithContext(ctx aws.Context, in *s3.GetObjectInput, _ ...request.Option) (*s3.GetObjectOutput, error) {
	if *in.Key == s3BadKey {
		return nil, errTest
	}

	return &s3.GetObjectOutput{}, nil
}

func TestS3GetBackendForURI(t *testing.T) {
	b, err := GetBackendForURI(AWSS3BackendPrefix + "://bucket_name")
	if err != nil {
		t.Errorf("Error while trying to get backend: %v", err)
	}
	if _, ok := b.(*AWSS3Backend); !ok {
		t.Errorf("Expected to get a backend of type AWSS3Backend, but did not.")
	}
}

func getOptions() []Option {
	// If we have a local minio target to test against, let's not use the mock clients
	if ok, _ := strconv.ParseBool(os.Getenv("S3_TEST_WITH_MINIO")); ok {
		return nil
	}
	return []Option{WithS3Client(&mockS3Client{}), WithS3Uploader(&mockS3Uploader{})}
}

func TestS3Init(t *testing.T) {
	testCases := []struct {
		conf    *BackendConfig
		errTest errTestFunc
		prefix  string
	}{
		{
			conf: &BackendConfig{
				TargetURI: AWSS3BackendPrefix + "://goodbucket",
			},
			errTest: nilErrTest,
		},
		{
			conf: &BackendConfig{
				TargetURI: AWSS3BackendPrefix + "://" + s3BadBucket,
			},
			errTest: errTestErrTest,
		},
		{
			conf: &BackendConfig{
				TargetURI: "nots3://goodbucket",
			},
			errTest: errInvalidURIErrTest,
		},
		{
			conf: &BackendConfig{
				TargetURI: AWSS3BackendPrefix + "://goodbucket/prefix",
			},
			errTest: nilErrTest,
			prefix:  "prefix",
		},
	}

	for idx, c := range testCases {
		b := &AWSS3Backend{}
		if err := b.Init(context.Background(), c.conf, getOptions()...); !c.errTest(err) {
			t.Errorf("%d: Did not get expected error, got %v instead", idx, err)
		}
		if b.prefix != c.prefix {
			t.Errorf("%d: Expected prefix %v, got %v", idx, c.prefix, b.prefix)
		}
	}
}

func TestS3Close(t *testing.T) {
	testCases := []struct {
		conf    *BackendConfig
		errTest errTestFunc
	}{
		{
			conf: &BackendConfig{
				TargetURI: AWSS3BackendPrefix + "://goodbucket",
			},
			errTest: nilErrTest,
		},
	}

	for idx, c := range testCases {
		b := &AWSS3Backend{}
		if err := b.Init(context.Background(), c.conf, getOptions()...); err != nil {
			t.Errorf("%d: Did not get expected nil error on Init, got %v instead", idx, err)
		}

		if err := b.Close(); !c.errTest(err) {
			t.Errorf("%d: Did not get expected error, got %v instead", idx, err)
		} else if err == nil {
			if b.client != nil {
				t.Errorf("%d: expected client to be nil after closing, but its not.", idx)
			}
			if b.uploader != nil {
				t.Errorf("%d: expected uploader to be nil after closing, but its not.", idx)
			}
		}
	}
}

func TestS3Delete(t *testing.T) {
	testCases := []struct {
		conf    *BackendConfig
		errTest errTestFunc
		key     string
	}{
		{
			conf: &BackendConfig{
				TargetURI: AWSS3BackendPrefix + "://goodbucket",
			},
			errTest: nilErrTest,
			key:     "goodkey",
		},
		{
			conf: &BackendConfig{
				TargetURI: AWSS3BackendPrefix + "://goodbucket",
			},
			errTest: errTestErrTest,
			key:     s3BadKey,
		},
	}

	for idx, c := range testCases {
		b := &AWSS3Backend{}
		if err := b.Init(context.Background(), c.conf, getOptions()...); err != nil {
			t.Errorf("%d: Did not get expected nil error on Init, got %v instead", idx, err)
		}
		if err := b.Delete(context.Background(), c.key); !c.errTest(err) {
			t.Errorf("%d: Did not get expected error, got %v instead", idx, err)
		}
	}
}

func TestS3Download(t *testing.T) {
	testCases := []struct {
		conf    *BackendConfig
		errTest errTestFunc
		key     string
	}{
		{
			conf: &BackendConfig{
				TargetURI: AWSS3BackendPrefix + "://goodbucket",
			},
			errTest: nilErrTest,
			key:     "goodkey",
		},
		{
			conf: &BackendConfig{
				TargetURI: AWSS3BackendPrefix + "://goodbucket",
			},
			errTest: errTestErrTest,
			key:     s3BadKey,
		},
	}

	for idx, c := range testCases {
		b := &AWSS3Backend{}
		if err := b.Init(context.Background(), c.conf, getOptions()...); err != nil {
			t.Errorf("%d: Did not get expected nil error on Init, got %v instead", idx, err)
		}
		if _, err := b.Download(context.Background(), c.key); !c.errTest(err) {
			t.Errorf("%d: Did not get expected error, got %v instead", idx, err)
		}
	}
}

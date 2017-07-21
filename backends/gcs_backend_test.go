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
	"crypto/rand"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/someone1/zfsbackup-go/helpers"
)

type gcsMockClient struct {
	bucketErr error
	err       error // For any function that returns an error, use this error
	reader    io.ReadCloser
	writer    io.WriteCloser
	list      []string
}

func (g *gcsMockClient) BucketExists(ctx context.Context, bucket string) error {
	return g.bucketErr
}

func (g *gcsMockClient) DeleteObject(ctx context.Context, bucket, object string) error {
	return g.err
}

func (g *gcsMockClient) NewWriter(ctx context.Context, bucket, object string, crc32Hash uint32) io.WriteCloser {
	return g.writer
}

func (g *gcsMockClient) NewReader(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	return g.reader, g.err
}

func (g *gcsMockClient) Close() error {
	return g.err
}

func (g *gcsMockClient) ListBucket(ctx context.Context, bucket, prefix string) ([]string, error) {
	return g.list, g.err
}

const (
	testBucketGood = GoogleCloudStorageBackendPrefix + "://bucketname"
)

var (
	validConfig = &BackendConfig{
		TargetURI: testBucketGood,
	}

	validClient = &gcsMockClient{
		err: nil,
	}
)

type gcsTestCase struct {
	client *gcsMockClient
	conf   *BackendConfig
}

func TestGCSGetBackendForURI(t *testing.T) {
	b, err := GetBackendForURI(testBucketGood)
	if err != nil {
		t.Errorf("Error while trying to get backend: %v", err)
	}
	if _, ok := b.(*GoogleCloudStorageBackend); !ok {
		t.Errorf("Expected to get a backend of type GoogleCloudStorageBackend, but did not.")
	}
}

// Bit of a misnomer as we aren't testing the actual Init function
func TestGCSInit(t *testing.T) {
	testCases := []struct {
		testcase gcsTestCase
		output   error
		prefix   string
	}{
		{
			testcase: gcsTestCase{
				client: validClient,
				conf:   validConfig,
			},
			output: nil,
		},
		{
			testcase: gcsTestCase{
				client: &gcsMockClient{
					bucketErr: errTest,
				},
				conf: validConfig,
			},
			output: errTest,
		},
		{
			testcase: gcsTestCase{
				client: validClient,
				conf: &BackendConfig{
					TargetURI: "notgs://bucketname",
				},
			},
			output: ErrInvalidURI,
		},
		{
			testcase: gcsTestCase{
				client: validClient,
				conf: &BackendConfig{
					TargetURI: "gs://bucketname/prefix",
				},
			},
			output: nil,
			prefix: "prefix",
		},
	}

	for idx, c := range testCases {
		b := &GoogleCloudStorageBackend{}
		if err := b.initWithClient(context.Background(), c.testcase.conf, c.testcase.client); err != c.output {
			t.Errorf("%d: Expected error %v, got %v", idx, c.output, err)
		}
		if b.prefix != c.prefix {
			t.Errorf("%d: Expected prefix %v, got %v", idx, c.prefix, b.prefix)
		}
	}

}
func TestGCSClose(t *testing.T) {
	testCases := []struct {
		testcase gcsTestCase
		output   error
		in       <-chan *helpers.VolumeInfo
	}{
		{
			testcase: gcsTestCase{
				client: validClient,
				conf:   validConfig,
			},
			output: nil,
		},
		{
			testcase: gcsTestCase{
				client: &gcsMockClient{
					err: errTest,
				},
				conf: validConfig,
			},
			output: errTest,
		},
	}

	for idx, c := range testCases {
		b := &GoogleCloudStorageBackend{}
		if err := b.initWithClient(context.Background(), c.testcase.conf, c.testcase.client); err != nil {
			t.Errorf("%d: error setting up backend - %v", idx, err)
		} else {
			err = b.Close()
			if err != c.output {
				t.Errorf("%d: Expected %v, got %v", idx, c.output, err)
			}
		}
	}
}

func TestGCSPreDownload(t *testing.T) {
	testCases := []struct {
		testcase gcsTestCase
		output   error
	}{
		{
			testcase: gcsTestCase{
				client: validClient,
				conf:   validConfig,
			},
			output: nil,
		},
		{
			testcase: gcsTestCase{
				client: &gcsMockClient{
					err: errTest,
				},
				conf: validConfig,
			},
			output: nil,
		},
	}

	for idx, c := range testCases {
		b := &GoogleCloudStorageBackend{}
		if err := b.initWithClient(context.Background(), c.testcase.conf, c.testcase.client); err != nil {
			t.Errorf("%d: error setting up backend - %v", idx, err)
		} else {
			err = b.PreDownload(context.Background(), nil)
			if err != c.output {
				t.Errorf("%d: Expected %v, got %v", idx, c.output, err)
			}
		}
	}
}

func TestGCSDelete(t *testing.T) {
	testCases := []struct {
		testcase gcsTestCase
		output   error
	}{
		{
			testcase: gcsTestCase{
				client: validClient,
				conf:   validConfig,
			},
			output: nil,
		},
		{
			testcase: gcsTestCase{
				client: &gcsMockClient{
					err: errTest,
				},
				conf: validConfig,
			},
			output: errTest,
		},
	}

	for idx, c := range testCases {
		b := &GoogleCloudStorageBackend{}
		if err := b.initWithClient(context.Background(), c.testcase.conf, c.testcase.client); err != nil {
			t.Errorf("%d: error setting up backend - %v", idx, err)
		} else {
			err = b.Delete(context.Background(), "")
			if err != c.output {
				t.Errorf("%d: Expected %v, got %v", idx, c.output, err)
			}
		}
	}
}

func TestGCSList(t *testing.T) {
	testList := []string{"l", "m", "n"}
	testCases := []struct {
		gcsTestCase
		output error
		list   []string
	}{
		{
			gcsTestCase: gcsTestCase{
				client: validClient,
				conf:   validConfig,
			},
			output: nil,
		},
		{
			gcsTestCase: gcsTestCase{
				client: &gcsMockClient{
					list: testList,
				},
				conf: validConfig,
			},
			output: nil,
			list:   testList,
		},
		{
			gcsTestCase: gcsTestCase{
				client: &gcsMockClient{
					err: errTest,
				},
				conf: validConfig,
			},
			output: errTest,
		},
	}

	for idx, c := range testCases {
		b := &GoogleCloudStorageBackend{}
		if err := b.initWithClient(context.Background(), c.conf, c.client); err != nil {
			t.Errorf("%d: error setting up backend - %v", idx, err)
		} else {
			list, lerr := b.List(context.Background(), "")
			if lerr != c.output {
				t.Errorf("%d: Expected error %v, got %v", idx, c.output, err)
			}
			if !reflect.DeepEqual(list, c.list) {
				t.Errorf("%d: Expected list %v, got %v", idx, c.list, list)
			}
		}
	}
}

func TestGCSGet(t *testing.T) {
	testPayLoad := make([]byte, 1024*1024)
	if _, err := rand.Read(testPayLoad); err != nil {
		t.Fatalf("could not read in random data for testing - %v", err)
	}
	reader := bytes.NewReader(testPayLoad)
	testCases := []struct {
		gcsTestCase
		output error
	}{
		{
			gcsTestCase: gcsTestCase{
				client: &gcsMockClient{
					reader: &closeReaderWrapper{reader},
				},
				conf: validConfig,
			},
			output: nil,
		},
		{
			gcsTestCase: gcsTestCase{
				client: &gcsMockClient{
					err: errTest,
				},
				conf: validConfig,
			},
			output: errTest,
		},
	}

	for idx, c := range testCases {
		b := &GoogleCloudStorageBackend{}
		if err := b.initWithClient(context.Background(), c.conf, c.client); err != nil {
			t.Errorf("%d: error setting up backend - %v", idx, err)
		} else {
			r, lerr := b.Get(context.Background(), "")
			if lerr != c.output {
				t.Errorf("%d: Expected error %v, got %v", idx, c.output, err)
			}
			if lerr == nil {
				testRead, terr := ioutil.ReadAll(r)
				if terr != nil {
					t.Errorf("%d: could not read from reader - %v", idx, terr)
				}
				if _, serr := reader.Seek(0, io.SeekStart); serr != nil {
					t.Fatalf("%d: could not seek buffer back to 0 - %v", idx, serr)
				}
				if !reflect.DeepEqual(testPayLoad, testRead) {
					t.Errorf("%d: read bytes not equal to given bytes", idx)
				}
			}
		}
	}
}

func TestGCSStartUpload(t *testing.T) {
	testPayLoad, goodvol, badvol, err := prepareTestVols()
	if err != nil {
		t.Fatalf("error preparing volume for testing - %v", err)
	}
	readVerify := bytes.NewBuffer(nil)

	testCases := []struct {
		gcsTestCase
		errTest errTestFunc
		vol     *helpers.VolumeInfo
	}{
		{
			gcsTestCase: gcsTestCase{
				client: &gcsMockClient{
					writer: &closeWriterWrapper{readVerify},
				},
				conf: &BackendConfig{
					TargetURI:               testBucketGood,
					MaxParallelUploads:      5,
					MaxBackoffTime:          1 * time.Second,
					MaxRetryTime:            5 * time.Second,
					MaxParallelUploadBuffer: make(chan bool, 1),
				},
			},
			errTest: nilErrTest,
			vol:     goodvol,
		},
		{
			gcsTestCase: gcsTestCase{
				client: &gcsMockClient{
					writer: &failWriter{},
					err:    errTest,
				},
				conf: &BackendConfig{
					TargetURI:               testBucketGood,
					MaxParallelUploads:      5,
					MaxBackoffTime:          1 * time.Second,
					MaxRetryTime:            5 * time.Second,
					MaxParallelUploadBuffer: make(chan bool, 1),
				},
			},
			errTest: errTestErrTest,
			vol:     goodvol,
		},
		{
			gcsTestCase: gcsTestCase{
				client: &gcsMockClient{
					writer: &closeWriterWrapper{readVerify},
				},
				conf: &BackendConfig{
					TargetURI:               testBucketGood,
					MaxParallelUploads:      5,
					MaxBackoffTime:          1 * time.Second,
					MaxRetryTime:            5 * time.Second,
					MaxParallelUploadBuffer: make(chan bool, 1),
				},
			},
			errTest: os.IsNotExist,
			vol:     badvol,
		},
	}

	for idx, c := range testCases {
		b := &GoogleCloudStorageBackend{}
		if err := b.initWithClient(context.Background(), c.conf, c.client); err != nil {
			t.Errorf("%d: error setting up backend - %v", idx, err)
		} else {
			readVerify.Reset()
			in := make(chan *helpers.VolumeInfo, 1)
			out := b.StartUpload(context.Background(), in)
			in <- c.vol
			close(in)
			outVol := <-out
			if errResult := b.Wait(); !c.errTest(errResult) {
				t.Errorf("%d: Unexpected error, got %v", idx, errResult)
			} else if errResult == nil {
				testRead := readVerify.Bytes()
				if !reflect.DeepEqual(testPayLoad, testRead) {
					t.Errorf("%d: read bytes not equal to given bytes", idx)
				}
				// Verify we got the same vol we passed in!
				if outVol != c.vol {
					t.Errorf("%d: did not get same volume passed in back out", idx)
				}
			}
		}
	}
}

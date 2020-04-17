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
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/someone1/zfsbackup-go/files"
)

var (
	errTest = errors.New("used for testing")
)

type errTestFunc func(error) bool

func nilErrTest(e error) bool           { return e == nil }
func errTestErrTest(e error) bool       { return e == errTest }
func errInvalidURIErrTest(e error) bool { return e == ErrInvalidURI }
func invalidByteErrTest(e error) bool {
	_, ok := e.(hex.InvalidByteError)
	return ok
}

func prepareTestVols() (payload []byte, goodVol, badVol *files.VolumeInfo, err error) {
	payload = make([]byte, 10*1024*1024)
	if _, err = rand.Read(payload); err != nil {
		return
	}
	reader := bytes.NewReader(payload)
	goodVol, err = files.CreateSimpleVolume(context.Background(), false)
	if err != nil {
		return
	}
	_, err = io.Copy(goodVol, reader)
	if err != nil {
		return
	}
	err = goodVol.Close()
	if err != nil {
		return
	}
	goodVol.ObjectName = strings.Join([]string{"this", "is", "just", "a", "test"}, "-") + ".ext"

	badVol, err = files.CreateSimpleVolume(context.Background(), false)
	if err != nil {
		return
	}
	err = badVol.Close()
	if err != nil {
		return
	}
	badVol.ObjectName = strings.Join([]string{"this", "is", "just", "a", "badtest"}, "-") + ".ext"

	err = badVol.DeleteVolume()

	return payload, goodVol, badVol, err
}

func TestGetBackendForURI(t *testing.T) {
	_, err := GetBackendForURI("thiswon'texist://")
	if err != ErrInvalidPrefix {
		t.Errorf("Expecting err %v, got %v for non-existent prefix", ErrInvalidPrefix, err)
	}

	_, err = GetBackendForURI("thisisinvalid")
	if err != ErrInvalidURI {
		t.Errorf("Expecting err %v, got %v for invalid URI", ErrInvalidURI, err)
	}
}

func BackendTest(ctx context.Context, prefix, uri string, skipPrefix bool, b Backend, opts ...Option) func(*testing.T) {
	return func(t *testing.T) {
		testPayLoad, goodVol, badVol, perr := prepareTestVols()
		if perr != nil {
			t.Fatalf("Error while creating test volumes: %v", perr)
		}
		defer func() {
			if err := goodVol.DeleteVolume(); err != nil {
				t.Errorf("could not delete good vol - %v", err)
			}
		}()

		t.Run("Init", func(t *testing.T) {
			// Bad TargetURI
			conf := &BackendConfig{
				TargetURI:               "notvalid://" + uri,
				UploadChunkSize:         8 * 1024 * 1024,
				MaxParallelUploads:      5,
				MaxParallelUploadBuffer: make(chan bool, 5),
			}
			err := b.Init(ctx, conf)
			if err != ErrInvalidURI {
				t.Fatalf("Expected Invalid URI error, got %v", err)
			}

			// Good TargetURI
			conf = &BackendConfig{
				TargetURI:               prefix + "://" + uri,
				UploadChunkSize:         8 * 1024 * 1024,
				MaxParallelUploads:      5,
				MaxParallelUploadBuffer: make(chan bool, 5),
			}
			err = b.Init(ctx, conf, opts...)
			if err != nil {
				t.Fatalf("Issue initilazing backend: %v", err)
			}

			if !skipPrefix {
				// Good TargetURI with URI prefix
				uri += "/prefix/"
				conf = &BackendConfig{
					TargetURI:               prefix + "://" + uri,
					UploadChunkSize:         8 * 1024 * 1024,
					MaxParallelUploads:      5,
					MaxParallelUploadBuffer: make(chan bool, 5),
				}
				err = b.Init(ctx, conf, opts...)
				if err != nil {
					t.Fatalf("Issue initilazing backend: %v", err)
				}
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

			err = b.Upload(ctx, badVol)
			if err == nil {
				t.Errorf("Expecting non-nil error uploading badvol, got nil instead.")
			}
		})

		t.Run("List", func(t *testing.T) {
			names, err := b.List(ctx, "")
			if err != nil {
				t.Fatalf("Issue listing backend: %v", err)
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
			err := b.PreDownload(ctx, []string{goodVol.ObjectName})
			if err != nil {
				t.Fatalf("Issue calling PreDownload: %v", err)
			}
		})

		t.Run("Download", func(t *testing.T) {
			r, err := b.Download(ctx, goodVol.ObjectName)
			if err != nil {
				t.Fatalf("Issue calling Download: %v", err)
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
				t.Errorf("Issue calling Delete: %v", err)
			}

			names, err := b.List(ctx, "")
			if err != nil {
				t.Errorf("Issue listing backend: %v", err)
			}

			if len(names) != 0 {
				t.Errorf("Expecting exactly zero names from list, got %d instead.", len(names))
			}
		})

		t.Run("Close", func(t *testing.T) {
			err := b.Close()
			if err != nil {
				t.Fatalf("Issue closing backend: %v", err)
			}
		})
	}
}

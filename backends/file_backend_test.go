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
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/someone1/zfsbackup-go/helpers"
)

func TestFileGetBackendForURI(t *testing.T) {
	b, err := GetBackendForURI(FileBackendPrefix + "://")
	if err != nil {
		t.Errorf("Error while trying to get backend: %v", err)
	}
	if _, ok := b.(*FileBackend); !ok {
		t.Errorf("Expected to get a backend of type FileBackend, but did not.")
	}
}

var (
	validFileConfig = &BackendConfig{TargetURI: FileBackendPrefix + "://" + os.TempDir()}
)

func TestFileInit(t *testing.T) {
	testCases := []struct {
		uri     string
		errTest errTestFunc
	}{
		{
			uri:     "file://",
			errTest: nilErrTest,
		},
		{
			uri:     "file:///",
			errTest: nilErrTest,
		},
		{
			uri:     "notvalid://",
			errTest: errInvalidPrefixErrTest,
		},
		{
			uri:     "file://this/should/not/exist",
			errTest: os.IsNotExist,
		},
		{
			uri:     "file:///dev/urandom",
			errTest: errInvalidURIErrTest,
		},
	}
	for idx, testCase := range testCases {
		b := &FileBackend{}
		conf := &BackendConfig{TargetURI: testCase.uri}
		if err := b.Init(context.Background(), conf); !testCase.errTest(err) {
			t.Errorf("%d: Unexpected error, got %v", idx, err)
		}
	}
}

func TestFileClose(t *testing.T) {
	b := &FileBackend{}
	if err := b.Init(context.Background(), validFileConfig); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		err = b.Close()
		if err != nil {
			t.Errorf("Expected %v, got %v", nil, err)
		}
	}

}

func TestFilePreDownload(t *testing.T) {
	b := &FileBackend{}
	if err := b.Init(context.Background(), validFileConfig); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		err = b.PreDownload(context.Background(), nil)
		if err != nil {
			t.Errorf("Expected nil error, got %v", err)
		}
	}
}

func TestFileDelete(t *testing.T) {
	// Let's create a file to delete
	w, err := ioutil.TempFile("", "filebackendtestfile")
	if err != nil {
		t.Errorf("Error trying to create a tempfile: %v", err)
	}
	defer os.Remove(w.Name())

	tempName := strings.TrimPrefix(w.Name(), os.TempDir())

	b := &FileBackend{}
	if err := b.Init(context.Background(), validFileConfig); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		err = b.Delete(context.Background(), tempName)
		if err != nil {
			t.Errorf("Expected nil error, got %v", err)
		}

		// Verify it happened
		if _, err = os.Stat(w.Name()); !os.IsNotExist(err) {
			t.Errorf("Expected does not exist error, got %v", err)
		}
	}
}

func TestFileList(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "filebackendtesttempdir")
	if err != nil {
		t.Errorf("Error trying to create a tempdir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := ioutil.TempFile(tempDir, "filebackendtestfile")
	if err != nil {
		t.Errorf("Error trying to create a tempfile: %v", err)
	}
	defer os.Remove(w.Name())
	w2, err := ioutil.TempFile(tempDir, "filebackendtestfileextended")
	if err != nil {
		t.Errorf("Error trying to create a tempfile: %v", err)
	}
	defer os.Remove(w2.Name())
	cleanName := strings.TrimPrefix(w.Name(), tempDir+string(filepath.Separator))
	cleanName2 := strings.TrimPrefix(w2.Name(), tempDir+string(filepath.Separator))

	config := &BackendConfig{
		TargetURI: "file://" + tempDir,
	}

	b := &FileBackend{}
	if err := b.Init(context.Background(), config); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		l, err := b.List(context.Background(), "")
		if err != nil {
			t.Errorf("Expected nil error, got %v", err)
		}

		if len(l) != 2 {
			t.Errorf("Expected only 2 files to list, got %d", len(l))
		}

		if l[0] != cleanName && l[1] != cleanName {
			t.Errorf("Could not find an expected file in the provided list.")
		}
		if l[0] != cleanName2 && l[1] != cleanName2 {
			t.Errorf("Could not find an expected file in the provided list.")
		}

		// Test with a known prefix
		l, err = b.List(context.Background(), "filebackendtestfileextended")
		if err != nil {
			t.Errorf("Expected nil error, got %v", err)
		}

		if len(l) != 1 {
			t.Errorf("Expected only 1 files to list, got %d", len(l))
		}

		if l[0] != cleanName2 {
			t.Errorf("Could not find an expected file in the provided list.")
		}
	}
}

func TestFileGet(t *testing.T) {
	// Let's create a file to Get
	w, err := ioutil.TempFile("", "filebackendtestfile")
	if err != nil {
		t.Errorf("Error trying to create a tempfile: %v", err)
	}
	defer os.Remove(w.Name())

	testPayLoad := make([]byte, 1024*1024)
	if _, err = rand.Read(testPayLoad); err != nil {
		t.Fatalf("could not read in random data for testing - %v", err)
	}
	reader := bytes.NewReader(testPayLoad)
	_, err = io.Copy(w, reader)
	if err != nil {
		t.Fatalf("could not write to temp file testing - %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("could not write temp file testing - %v", err)
	}

	tempName := strings.TrimPrefix(w.Name(), os.TempDir())

	b := &FileBackend{}
	if err := b.Init(context.Background(), validFileConfig); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		r, lerr := b.Get(context.Background(), tempName)
		if lerr != nil {
			t.Errorf("Expected nil error, got %v", lerr)
		} else {
			testRead, terr := ioutil.ReadAll(r)
			if terr != nil {
				t.Errorf("could not read from reader - %v", terr)
			}
			// Verify we got the right data back
			if !reflect.DeepEqual(testPayLoad, testRead) {
				t.Errorf("read bytes not equal to given bytes")
			}
		}
	}
}

func TestFileStartUpload(t *testing.T) {
	testPayLoad, goodVol, badVol, err := prepareTestVols()
	if err != nil {
		t.Fatalf("error preparing volumes for testing - %v", err)
	}

	tempDir, terr := ioutil.TempDir("", "zfsbackupfilebackendtest")
	if terr != nil {
		t.Fatalf("error preparing temp dir for rests - %v", terr)
	}
	//defer os.RemoveAll(tempDir) // clean up

	config := &BackendConfig{
		TargetURI:               "file://" + tempDir,
		MaxBackoffTime:          1 * time.Second,
		MaxRetryTime:            5 * time.Second,
		MaxParallelUploads:      5,
		MaxParallelUploadBuffer: make(chan bool, 1),
	}

	testCases := []struct {
		vol   *helpers.VolumeInfo
		valid func(error) bool
	}{
		{
			vol:   goodVol,
			valid: nilErrTest,
		},
		{
			vol:   badVol,
			valid: os.IsNotExist,
		},
	}

	goodVol.ObjectName = strings.Join([]string{"this", "is", "just", "a", "test"}, "|") + ".ext"
	for idx, testCase := range testCases {

		b := &FileBackend{}
		if err := b.Init(context.Background(), config); err != nil {
			t.Errorf("%d: Expected error %v, got %v", idx, nil, err)
		} else {
			in := make(chan *helpers.VolumeInfo, 1)
			out := b.StartUpload(context.Background(), in)
			in <- testCase.vol
			close(in)
			outVol := <-out
			if errResult := b.Wait(); !testCase.valid(errResult) {
				t.Errorf("%d: error %v id not pass validation function", idx, errResult)
			} else if errResult == nil {
				// Verify we have a file written where we expect it to be, read it in to be sure its the contents we want it to be
				readVerify, rerr := ioutil.ReadFile(filepath.Join(tempDir, testCase.vol.ObjectName))
				if rerr != nil {
					t.Errorf("%d: expected file does not exist as we expect it to - %v", idx, rerr)
				} else {
					if !reflect.DeepEqual(testPayLoad, readVerify) {
						t.Errorf("%d: read bytes not equal to given bytes", idx)
					}
				}
				// Verify we got the same vol we passed in!
				if outVol != testCase.vol {
					t.Errorf("did not get same volume passed in back out")
				}
			}
		}
	}
}

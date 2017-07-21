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
	"io/ioutil"
	"os"
	"strings"
	"testing"
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

// func TestFileList(t *testing.T) {
// 	tempDir, err := ioutil.TempDir()
// 	w, err := ioutil.TempFile("filebackendtestfile", "filebackendtestfile")
// 	if err != nil {
// 		t.Errorf("Error trying to create a tempfile: %v", err)
// 	}
// 	defer os.Remove(w.Name())
// 	w2, err := ioutil.TempFile("filebackendtestfile", "filebackendtestfileextended")
// 	if err != nil {
// 		t.Errorf("Error trying to create a tempfile: %v", err)
// 	}
// 	defer os.Remove(w.Name())
// 	defer os.Remove(w2.Name())
// 	trimPrefix := filepath.Join(os.TempDir(), "filebackendtestfile")

// 	b := &FileBackend{}
// 	if err := b.Init(context.Background(), nil); err != nil {
// 		t.Errorf("Expected error %v, got %v", nil, err)
// 	} else {
// 		l, err = b.List(context.Background(), "filebackendtestfile")
// 		if err != nil {
// 			t.Errorf("Expected nil error, got %v", err)
// 		}

// 		if len(l) != 2 {
// 			t.Errorf("Expected only 2 files to list, got %d", len(l))
// 		}

// 		if l[0] ==

// 	}
// }

// func TestFileGet(t *testing.T) {
// 	b := &DeleteBackend{}
// 	if err := b.Init(context.Background(), nil); err != nil {
// 		t.Errorf("Expected error %v, got %v", nil, err)
// 	} else {
// 		_, err = b.Get(context.Background(), "")
// 		if err == nil {
// 			t.Errorf("Expected non-nil error, got %v", err)
// 		}
// 	}
// }

// func TestFileStartUpload(t *testing.T) {
// 	_, goodVol, badVol, err := prepareTestVols()
// 	if err != nil {
// 		t.Fatalf("error preparing good volume for testing - %v", err)
// 	}

// 	config := &BackendConfig{
// 		TargetURI:      "delete://",
// 		MaxBackoffTime: 1 * time.Second,
// 		MaxRetryTime:   5 * time.Second,
// 	}

// 	testCases := []struct {
// 		vol   *helpers.VolumeInfo
// 		valid func(error) bool
// 	}{
// 		{
// 			vol:   goodVol,
// 			valid: func(e error) bool { return e == nil },
// 		},
// 		{
// 			vol:   badVol,
// 			valid: os.IsNotExist,
// 		},
// 	}

// 	for idx, testCase := range testCases {

// 		b := &DeleteBackend{}
// 		if err := b.Init(context.Background(), config); err != nil {
// 			t.Errorf("%d: Expected error %v, got %v", idx, nil, err)
// 		} else {
// 			in := make(chan *helpers.VolumeInfo, 1)
// 			out := b.StartUpload(context.Background(), in)
// 			in <- testCase.vol
// 			close(in)
// 			outVol := <-out
// 			if errResult := b.Wait(); !testCase.valid(errResult) {
// 				t.Errorf("%d: error %v id not pass validation function", idx, errResult)
// 			} else if errResult == nil {
// 				err = testCase.vol.OpenVolume()
// 				if !os.IsNotExist(err) {
// 					t.Errorf("expected not exist error, got %v instead", err)
// 				}
// 				// Verify we got the same vol we passed in!
// 				if outVol != testCase.vol {
// 					t.Errorf("did not get same volume passed in back out")
// 				}
// 			}
// 		}
// 	}
// }

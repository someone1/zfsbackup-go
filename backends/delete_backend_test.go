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
	"time"

	"github.com/someone1/zfsbackup-go/helpers"
)

func TestDeleteGetBackendForURI(t *testing.T) {
	b, err := GetBackendForURI(DeleteBackendPrefix + "://")
	if err != nil {
		t.Errorf("Error while trying to get backend: %v", err)
	}
	if _, ok := b.(*DeleteBackend); !ok {
		t.Errorf("Expected to get a backend of type DeleteBackend, but did not.")
	}
}

func TestDeleteInit(t *testing.T) {
	b := &DeleteBackend{}
	if err := b.Init(context.Background(), nil); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	}
}
func TestDeleteClose(t *testing.T) {
	b := &DeleteBackend{}
	if err := b.Init(context.Background(), nil); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		err = b.Close()
		if err != nil {
			t.Errorf("Expected %v, got %v", nil, err)
		}
	}

}

func TestDeletePreDownload(t *testing.T) {
	b := &DeleteBackend{}
	if err := b.Init(context.Background(), nil); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		err = b.PreDownload(context.Background(), nil)
		if err == nil {
			t.Errorf("Expected non-nil error, got %v", err)
		}
	}
}

func TestDeleteDelete(t *testing.T) {
	b := &DeleteBackend{}
	if err := b.Init(context.Background(), nil); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		err = b.Delete(context.Background(), "")
		if err == nil {
			t.Errorf("Expected non-nil error, got %v", err)
		}
	}
}

func TestDeleteList(t *testing.T) {
	b := &DeleteBackend{}
	if err := b.Init(context.Background(), nil); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		_, err = b.List(context.Background(), "")
		if err == nil {
			t.Errorf("Expected non-nil error, got %v", err)
		}
	}
}

func TestDeleteDownload(t *testing.T) {
	b := &DeleteBackend{}
	if err := b.Init(context.Background(), nil); err != nil {
		t.Errorf("Expected error %v, got %v", nil, err)
	} else {
		_, err = b.Download(context.Background(), "")
		if err == nil {
			t.Errorf("Expected non-nil error, got %v", err)
		}
	}
}

func TestDeleteStartUpload(t *testing.T) {
	_, goodVol, badVol, err := prepareTestVols()
	if err != nil {
		t.Fatalf("error preparing good volume for testing - %v", err)
	}

	config := &BackendConfig{
		TargetURI:      "delete://",
		MaxBackoffTime: 1 * time.Second,
		MaxRetryTime:   5 * time.Second,
	}

	testCases := []struct {
		vol   *helpers.VolumeInfo
		valid errTestFunc
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

	for idx, testCase := range testCases {
		b := &DeleteBackend{}
		if err := b.Init(context.Background(), config); err != nil {
			t.Errorf("%d: Expected error %v, got %v", idx, nil, err)
		} else {
			if errResult := b.Upload(context.Background(), testCase.vol); !testCase.valid(errResult) {
				t.Errorf("%d: error %v id not pass validation function", idx, errResult)
			} else if errResult == nil {
				err = testCase.vol.OpenVolume()
				if !os.IsNotExist(err) {
					t.Errorf("expected not exist error, got %v instead", err)
				}
			}
		}
	}
}

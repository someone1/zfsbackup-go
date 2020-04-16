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
			errTest: errInvalidURIErrTest,
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

func TestFileBackend(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempPath, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("could not create temp dir: %v", err)
	}

	defer func() {
		if err = os.RemoveAll(tempPath); err != nil {
			t.Errorf("could not clearn temp dir: %v", err)
		}
	}()

	b, err := GetBackendForURI(FileBackendPrefix + "://" + tempPath)
	if err != nil {
		t.Fatalf("Error while trying to get backend: %v", err)
	}

	BackendTest(ctx, FileBackendPrefix, tempPath, true, b)(t)
}

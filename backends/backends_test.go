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
	"errors"
	"io"
	"testing"

	"github.com/someone1/zfsbackup-go/helpers"
)

var (
	errTest = errors.New("used for testing")
)

type closeReaderWrapper struct {
	r io.ReadSeeker
}

func (c *closeReaderWrapper) Read(b []byte) (int, error) {
	return c.r.Read(b)
}

func (c *closeReaderWrapper) Close() error {
	return nil
}

func (c *closeReaderWrapper) Seek(offset int64, whence int) (int64, error) {
	return c.r.Seek(offset, whence)
}

type closeWriterWrapper struct {
	w io.Writer
}

func (c *closeWriterWrapper) Close() error {
	return nil
}

func (c *closeWriterWrapper) Write(p []byte) (int, error) {
	return c.w.Write(p)
}

type failWriter struct {
}

func (f *failWriter) Close() error {
	return nil
}

func (f *failWriter) Write(p []byte) (int, error) {
	return 0, errTest
}

type errTestFunc func(error) bool

func nilErrTest(e error) bool              { return e == nil }
func errTestErrTest(e error) bool          { return e == errTest }
func errInvalidPrefixErrTest(e error) bool { return e == ErrInvalidPrefix }
func errInvalidURIErrTest(e error) bool    { return e == ErrInvalidURI }
func nonNilErrTest(e error) bool {return e != nil }

func prepareTestVols() (payload []byte, goodVol *helpers.VolumeInfo, badVol *helpers.VolumeInfo, err error) {
	payload = make([]byte, 10*1024*1024)
	if _, err = rand.Read(payload); err != nil {
		return
	}
	reader := bytes.NewReader(payload)
	goodVol, err = helpers.CreateSimpleVolume(context.Background(), false)
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

	badVol, err = helpers.CreateSimpleVolume(context.Background(), false)
	if err != nil {
		return
	}
	err = badVol.Close()
	if err != nil {
		return
	}

	err = badVol.DeleteVolume()

	return
}

func TestGetBackendForURI(t *testing.T) {
	_, err := GetBackendForURI("thiswon'texist://")
	if err != ErrInvalidPrefix {
		t.Errorf("Expecting err %v, got %v for non-existant prefix", ErrInvalidPrefix, err)
	}

	_, err = GetBackendForURI("thisisinvalid")
	if err != ErrInvalidURI {
		t.Errorf("Expecting err %v, got %v for invalid URI", ErrInvalidURI, err)
	}
}

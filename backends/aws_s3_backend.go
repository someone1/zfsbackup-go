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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/someone1/zfsbackup-go/helpers"
)

// AWSS3BackendPrefix is the URI prefix used for the AWSS3Backend.
const AWSS3BackendPrefix = "s3"

// AWSS3Backend integrates with Amazon Web Services' S3.
type AWSS3Backend struct {
	conf       *BackendConfig
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	sess       *session.Session
	client     *s3.S3
	prefix     string
	bucketName string
}

// Authenticate https://godoc.org/github.com/aws/aws-sdk-go/aws/session#hdr-Environment_Variables

// Init will initialize the AWSS3Backend and verify the provided URI is valid/exists.
func (a *AWSS3Backend) Init(ctx context.Context, conf *BackendConfig) error {
	a.conf = conf
	a.ctx, a.cancel = context.WithCancel(ctx)

	if !validURI.MatchString(a.conf.TargetURI) {
		return ErrInvalidURI
	}

	cleanPrefix := strings.TrimPrefix(a.conf.TargetURI, "s3://")
	if cleanPrefix == a.conf.TargetURI {
		return ErrInvalidURI
	}

	uriParts := strings.Split(cleanPrefix, "/")
	if len(uriParts) < 1 {
		return ErrInvalidURI
	}

	a.bucketName = uriParts[0]
	if len(uriParts) > 1 {
		a.prefix = strings.Join(uriParts[1:], "/")
	}

	sess, err := session.NewSession()
	if err != nil {
		return err
	}

	a.sess = sess
	a.client = s3.New(sess)

	listReq := &s3.ListObjectsV2Input{
		Bucket:  aws.String(a.bucketName),
		MaxKeys: aws.Int64(0),
	}

	_, err = a.client.ListObjectsV2(listReq)
	return err
}

// StartUpload will begin the S3 upload workers
func (a *AWSS3Backend) StartUpload(in <-chan *helpers.VolumeInfo) <-chan *helpers.VolumeInfo {
	out := make(chan *helpers.VolumeInfo)
	a.wg.Add(a.conf.MaxParallelUploads)
	for i := 0; i < a.conf.MaxParallelUploads; i++ {
		go func() {
			uploader(a.ctx, a.uploadWrapper, "s3", a.conf.getExpBackoff(), in, out)
			a.wg.Done()
		}()
	}

	go func() {
		a.Wait()
		close(out)
	}()

	return out
}

func WithContentMD5Header(md5sum string) request.Option {
	return func(r *request.Request) {
		r.HTTPRequest.Header.Set("Content-MD5", md5sum)
	}
}

func (a *AWSS3Backend) uploadWrapper(vol *helpers.VolumeInfo) func() error {
	// TODO: Enable resumeable uploads
	return func() error {
		select {
		case <-a.ctx.Done():
			return nil
		default:
			a.conf.MaxParallelUploadBuffer <- true
			defer func() {
				<-a.conf.MaxParallelUploadBuffer
			}()

			if err := vol.OpenVolume(); err != nil {
				helpers.AppLogger.Debugf("s3 backend: Error while opening volume %s - %v", vol.ObjectName, err)
				return err
			}
			defer vol.Close()
			md5Raw, err := hex.DecodeString(vol.MD5Sum)
			if err != nil {
				return err
			}
			b64md5 := base64.StdEncoding.EncodeToString(md5Raw)
			key := a.prefix + vol.ObjectName
			_, err = a.client.PutObjectWithContext(a.ctx, &s3.PutObjectInput{
				Bucket: aws.String(a.bucketName),
				Key:    aws.String(key),
				Body:   vol,
			}, WithContentMD5Header(b64md5))

			if err != nil {
				helpers.AppLogger.Debugf("s3 backend: Error while uploading volume %s - %v", vol.ObjectName, err)
				// Check if the context was canceled, and if so, don't let the backoff function retry
				select {
				case <-a.ctx.Done():
					return nil
				default:
				}
			}
			return err
		}
	}
}

// Delete will delete the given object from the configured bucket
func (a *AWSS3Backend) Delete(key string) error {
	_, err := a.client.DeleteObjectWithContext(a.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(a.bucketName),
		Key:    aws.String(key),
	})

	return err
}

// PreDownload will restore objects from Glacier as required.
func (a *AWSS3Backend) PreDownload(keys []string) error {
	// First Let's check if any objects are on the GLACIER storage class
	toRestore := make([]string, 0, len(keys))
	restoreTier := os.Getenv("AWS_S3_GLACIER_RESTORE_TIER")
	if restoreTier == "" {
		restoreTier = s3.TierBulk
	}
	helpers.AppLogger.Debugf("s3 backend: will use the %s restore tier when trying to restore from Glacier.", restoreTier)
	for _, key := range keys {
		resp, err := a.client.HeadObjectWithContext(a.ctx, &s3.HeadObjectInput{
			Bucket: aws.String(a.bucketName),
			Key:    aws.String(key),
		})
		if err != nil {
			return err
		}
		if resp.StorageClass == aws.String(s3.ObjectStorageClassGlacier) {
			helpers.AppLogger.Debugf("s3 backend: key %s will be restored from the Glacier storage class.", key)
			// Let's Start a restore
			toRestore = append(toRestore, key)
			_, rerr := a.client.RestoreObjectWithContext(a.ctx, &s3.RestoreObjectInput{
				Bucket: aws.String(a.bucketName),
				Key:    aws.String(key),
				RestoreRequest: &s3.RestoreRequest{
					Days: aws.Int64(3),
					GlacierJobParameters: &s3.GlacierJobParameters{
						Tier: aws.String(restoreTier),
					},
				},
			})
			if rerr != nil {
				if aerr, ok := rerr.(awserr.Error); ok && aerr.Code() != "RestoreAlreadyInProgress" {
					helpers.AppLogger.Debugf("s3 backend: error trying to restore key %s - %s: %s", key, aerr.Code(), aerr.Message())
					return rerr
				}
			}
		}
	}
	// Now wait for the objects to be restored
	for idx := 0; idx < len(toRestore); idx++ {
		key := toRestore[idx]
		resp, err := a.client.HeadObjectWithContext(a.ctx, &s3.HeadObjectInput{
			Bucket: aws.String(a.bucketName),
			Key:    aws.String(key),
		})
		if err != nil {
			return err
		}
		if resp.Restore == aws.String("ongoing-request=\"true\"") {
			time.Sleep(5 * time.Minute)
			idx--
		} else {
			helpers.AppLogger.Debugf("s3 backend: key %s restored.", key)
		}
	}
	return nil
}

// Get will download the requseted object
func (a *AWSS3Backend) Get(key string) (io.ReadCloser, error) {
	resp, err := a.client.GetObjectWithContext(a.ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Wait will wait until all volumes have been processed from the incoming
// channel.
func (a *AWSS3Backend) Wait() {
	a.wg.Wait()
}

// Close will signal the upload workers to stop processing the contents of the incoming channel
// and to close the outgoing channel. It will also cancel any ongoing requests.
func (a *AWSS3Backend) Close() error {
	a.cancel()
	a.Wait()
	a.client = nil
	return nil
}

// List will iterate through all objects in the configured GCS bucket and return
// a list of object names.
func (a *AWSS3Backend) List(prefix string) ([]string, error) {
	resp, err := a.client.ListObjectsV2WithContext(a.ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(a.bucketName),
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	l := make([]string, 0, 1000)
	for {
		for _, obj := range resp.Contents {
			l = append(l, *obj.Key)
		}

		if !*resp.IsTruncated {
			break
		}

		resp, err = a.client.ListObjectsV2WithContext(a.ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(a.bucketName),
			MaxKeys:           aws.Int64(1000),
			Prefix:            aws.String(prefix),
			ContinuationToken: resp.NextContinuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("gs backend: could not list bucket due to error - %v", err)
		}
	}

	return l, nil
}

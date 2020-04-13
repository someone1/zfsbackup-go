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
	"crypto/md5" //nolint:gosec // MD5 not used cryptographically here
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"

	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
)

// AWSS3BackendPrefix is the URI prefix used for the AWSS3Backend.
const AWSS3BackendPrefix = "s3"

// AWSS3Backend integrates with Amazon Web Services' S3.
type AWSS3Backend struct {
	conf       *BackendConfig
	mutex      sync.Mutex
	client     s3iface.S3API
	uploader   s3manageriface.UploaderAPI
	prefix     string
	bucketName string
}

// Authenticate https://godoc.org/github.com/aws/aws-sdk-go/aws/session#hdr-Environment_Variables

type logger struct{}

func (l logger) Log(args ...interface{}) {
	log.AppLogger.Debugf("s3 backend:", args...)
}

type withS3Client struct{ client s3iface.S3API }

func (w withS3Client) Apply(b Backend) {
	if v, ok := b.(*AWSS3Backend); ok {
		v.client = w.client
	}
}

// WithS3Client will override an S3 backend's underlying API client with the one provided.
// Primarily used to inject mock clients for testing.
func WithS3Client(c s3iface.S3API) Option {
	return withS3Client{c}
}

type withS3Uploader struct{ uploader s3manageriface.UploaderAPI }

func (w withS3Uploader) Apply(b Backend) {
	if v, ok := b.(*AWSS3Backend); ok {
		v.uploader = w.uploader
	}
}

// WithS3Uploader will override an S3 backend's underlying uploader client with the one provided.
// Primarily used to inject mock clients for testing.
func WithS3Uploader(c s3manageriface.UploaderAPI) Option {
	return withS3Uploader{c}
}

// Init will initialize the AWSS3Backend and verify the provided URI is valid/exists.
func (a *AWSS3Backend) Init(ctx context.Context, conf *BackendConfig, opts ...Option) error {
	a.conf = conf

	cleanPrefix := strings.TrimPrefix(a.conf.TargetURI, AWSS3BackendPrefix+"://")
	if cleanPrefix == a.conf.TargetURI {
		return ErrInvalidURI
	}

	uriParts := strings.Split(cleanPrefix, "/")

	a.bucketName = uriParts[0]
	if len(uriParts) > 1 {
		a.prefix = strings.Join(uriParts[1:], "/")
	}

	for _, opt := range opts {
		opt.Apply(a)
	}

	if a.client == nil {
		awsconf := aws.NewConfig().
			WithS3ForcePathStyle(true).
			WithEndpoint(os.Getenv("AWS_S3_CUSTOM_ENDPOINT"))
		if enableDebug, _ := strconv.ParseBool(os.Getenv("AWS_S3_ENABLE_DEBUG")); enableDebug {
			awsconf = awsconf.WithLogger(logger{}).
				WithLogLevel(aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors)
		}

		sess, err := session.NewSession(awsconf)
		if err != nil {
			return err
		}

		a.client = s3.New(sess)
	}

	if a.uploader == nil {
		a.uploader = s3manager.NewUploaderWithClient(a.client, func(u *s3manager.Uploader) {
			u.Concurrency = conf.MaxParallelUploads
		}, func(u *s3manager.Uploader) {
			u.PartSize = int64(conf.UploadChunkSize)
		})
	}

	listReq := &s3.ListObjectsV2Input{
		Bucket:  aws.String(a.bucketName),
		MaxKeys: aws.Int64(0),
	}

	_, err := a.client.ListObjectsV2WithContext(ctx, listReq)
	return err
}

func withContentMD5Header(md5sum string) request.Option {
	return func(ro *request.Request) {
		if md5sum != "" {
			ro.Handlers.Build.PushBack(func(r *request.Request) {
				r.HTTPRequest.Header.Set("Content-MD5", md5sum)
			})
		}
	}
}

func withRequestLimiter(buffer chan bool) request.Option {
	return func(ro *request.Request) {
		ro.Handlers.Send.PushFront(func(r *request.Request) {
			buffer <- true
		})

		ro.Handlers.Send.PushBack(func(r *request.Request) {
			<-buffer
		})
	}
}

func withComputeMD5HashHandler(ro *request.Request) {
	ro.Handlers.Build.PushBack(func(r *request.Request) {
		reader := r.GetBody()
		if reader == nil {
			return
		}

		//nolint:gosec // MD5 not used cryptographically here
		md5Raw := md5.New()
		_, err := io.Copy(md5Raw, reader)
		if err != nil {
			r.Error = err
			return
		}
		_, r.Error = reader.Seek(0, io.SeekStart)
		b64md5 := base64.StdEncoding.EncodeToString(md5Raw.Sum(nil))
		r.HTTPRequest.Header.Set("Content-MD5", b64md5)
	})
}

type reader struct {
	r io.Reader
}

func (r *reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

// Upload will upload the provided volume to this AWSS3Backend's configured bucket+prefix
// It utilizes multipart uploads to upload a single file in chunks concurrently. Impartial
// uploads are cleaned up by the s3manager provided by the AWS Go SDK, but users can also
// implement lifecycle rules, see:
// https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html#mpu-abort-incomplete-mpu-lifecycle-config
func (a *AWSS3Backend) Upload(ctx context.Context, vol *files.VolumeInfo) error {
	// We will achieve parallel upload by splitting a single upload into chunks
	// so don't let multiple calls to this function run in parallel.
	a.mutex.Lock()
	defer a.mutex.Unlock()

	key := a.prefix + vol.ObjectName
	var options []request.Option
	options = append(options, withRequestLimiter(a.conf.MaxParallelUploadBuffer))
	var r io.Reader

	if !vol.IsUsingPipe() {
		r = vol
		if vol.Size < uint64(s3manager.MinUploadPartSize) {
			// It will not chunk the upload so we already know the md5 of the content
			md5Raw, merr := hex.DecodeString(vol.MD5Sum)
			if merr != nil {
				return merr
			}
			b64md5 := base64.StdEncoding.EncodeToString(md5Raw)
			options = append(options, withContentMD5Header(b64md5))
		} else {
			options = append(options, withComputeMD5HashHandler)
		}
	} else {
		r = &reader{vol} // Remove the Seek interface since we are using a Pipe
	}

	// Do a MultiPart Upload - force the s3manager to compute each chunks md5 hash
	_, err := a.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(a.bucketName),
		Key:    aws.String(key),
		Body:   r,
	}, s3manager.WithUploaderRequestOptions(options...))

	if err != nil {
		log.AppLogger.Debugf("s3 backend: Error while uploading volume %s - %v", vol.ObjectName, err)
	}
	return err
}

// Delete will delete the given object from the configured bucket
func (a *AWSS3Backend) Delete(ctx context.Context, key string) error {
	_, err := a.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(a.bucketName),
		Key:    aws.String(key),
	})

	return err
}

// PreDownload will restore objects from Glacier as required.
func (a *AWSS3Backend) PreDownload(ctx context.Context, keys []string) error {
	// First Let's check if any objects are on the GLACIER storage class
	toRestore := make([]string, 0, len(keys))
	restoreTier := os.Getenv("AWS_S3_GLACIER_RESTORE_TIER")
	if restoreTier == "" {
		restoreTier = s3.TierBulk
	}
	var bytesToRestore int64
	log.AppLogger.Debugf("s3 backend: will use the %s restore tier when trying to restore from Glacier.", restoreTier)
	for _, key := range keys {
		resp, err := a.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(a.bucketName),
			Key:    aws.String(key),
		})
		if err != nil {
			return err
		}
		if resp.StorageClass != nil && *resp.StorageClass == s3.ObjectStorageClassGlacier {
			log.AppLogger.Debugf("s3 backend: key %s will be restored from the Glacier storage class.", key)
			bytesToRestore += *resp.ContentLength
			// Let's Start a restore
			toRestore = append(toRestore, key)
			_, rerr := a.client.RestoreObjectWithContext(ctx, &s3.RestoreObjectInput{
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
					log.AppLogger.Debugf("s3 backend: error trying to restore key %s - %s: %s", key, aerr.Code(), aerr.Message())
					return rerr
				}
			}
		}
	}
	if len(toRestore) > 0 {
		log.AppLogger.Infof(
			"s3 backend: waiting for %d objects to restore from Glacier totaling %d bytes (this could take several hours)",
			len(toRestore), bytesToRestore,
		)
		// Now wait for the objects to be restored
		backoffCount := 1
		for idx := 0; idx < len(toRestore); idx++ {
			key := toRestore[idx]
			resp, err := a.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(a.bucketName),
				Key:    aws.String(key),
			})
			if err != nil {
				return err
			}
			if *resp.Restore == "ongoing-request=\"true\"" {
				time.Sleep(time.Duration(backoffCount) * time.Minute)
				idx--
				backoffCount++
				if backoffCount > 10 {
					backoffCount = 10
				}
			} else {
				backoffCount = 1
				log.AppLogger.Debugf("s3 backend: key %s restored.", key)
			}
		}
	}
	return nil
}

// Download will download the requseted object which can be read from the returned io.ReadCloser
func (a *AWSS3Backend) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := a.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Close will release any resources used by the AWS S3 backend.
func (a *AWSS3Backend) Close() error {
	a.client = nil
	a.uploader = nil
	return nil
}

// List will iterate through all objects in the configured AWS S3 bucket and return
// a list of keys, filtering by the provided prefix.
func (a *AWSS3Backend) List(ctx context.Context, prefix string) ([]string, error) {
	resp, err := a.client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
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

		resp, err = a.client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(a.bucketName),
			MaxKeys:           aws.Int64(1000),
			Prefix:            aws.String(prefix),
			ContinuationToken: resp.NextContinuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("s3 backend: could not list bucket due to error - %v", err)
		}
	}

	return l, nil
}

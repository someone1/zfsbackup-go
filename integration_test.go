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

package main

import (
	"os"
	"os/exec"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/someone1/zfsbackup-go/backends"
	"github.com/someone1/zfsbackup-go/cmd"
	"github.com/someone1/zfsbackup-go/helpers"
)

const s3TestBucketName = "s3buckettest"

func TestIntegration(t *testing.T) {
	if os.Getenv("AWS_S3_CUSTOM_ENDPOINT") == "" {
		t.Skip("No custom S3 Endpoint provided to test against")
	}

	awsconf := aws.NewConfig().
		WithS3ForcePathStyle(true).
		WithEndpoint(os.Getenv("AWS_S3_CUSTOM_ENDPOINT"))

	sess, err := session.NewSession(awsconf)
	if err != nil {
		t.Fatalf("could not create AWS client due to error: %v", err)
	}

	client := s3.New(sess)
	_, err = client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(s3TestBucketName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() != "BucketAlreadyOwnedByYou" {
				t.Fatalf("could not create S3 bucket due to error: %v", err)
			}
		}
	}

	defer client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(s3TestBucketName),
	})

	t.Run("Backup", func(t *testing.T) {
		os.Args = []string{helpers.ProgramName, "send", "--logLevel", "debug", "tank/data@a", backends.AWSS3BackendPrefix + "://" + s3TestBucketName}
		if err := cmd.RootCmd.Execute(); err != nil {
			t.Fatalf("error performing backup")
		}
	})

	t.Run("Restore", func(t *testing.T) {
		os.Args = []string{helpers.ProgramName, "receive", "--logLevel", "debug", "-F", "tank/data@a", backends.AWSS3BackendPrefix + "://" + s3TestBucketName, "tank/data2"}
		if err := cmd.RootCmd.Execute(); err != nil {
			t.Fatalf("error performing receive")
		}

		cmd := exec.Command("diff", "-rq", "/tank/data", "/tank/data2")
		err := cmd.Run()
		if err != nil {
			t.Fatalf("unexpected difference comparing the restored backup: %v", err)
		}
	})

}

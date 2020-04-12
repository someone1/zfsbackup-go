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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/someone1/zfsbackup-go/backends"
	"github.com/someone1/zfsbackup-go/backup"
	"github.com/someone1/zfsbackup-go/cmd"
)

const s3TestBucketName = "s3integrationbuckettest"
const azureTestBucketName = "azureintegrationbuckettest"
const logLevel = "debug"

func setupAzureBucket(t *testing.T) func() {
	t.Helper()
	if os.Getenv("AZURE_CUSTOM_ENDPOINT") == "" {
		t.Skip("No custom Azure Endpoint provided to test against")
	}
	err := os.Setenv("AZURE_ACCOUNT_NAME", storage.StorageEmulatorAccountName)
	if err != nil {
		t.Fatalf("could not set environmental variable due to error: %v", err)
	}
	err = os.Setenv("AZURE_ACCOUNT_KEY", storage.StorageEmulatorAccountKey)
	if err != nil {
		t.Fatalf("could not set environmental variable due to error: %v", err)
	}

	ctx := context.Background()

	credential, err := azblob.NewSharedKeyCredential(storage.StorageEmulatorAccountName, storage.StorageEmulatorAccountKey)
	if err != nil {
		t.Fatalf("failed to parse SAS key: %v", err)
	}
	destURL, err := url.Parse(os.Getenv("AZURE_CUSTOM_ENDPOINT"))
	if err != nil {
		t.Fatalf("failed to construct Azure API URL: %v", err)
	}
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	svcURL := azblob.NewServiceURL(*destURL, pipeline)
	containerSvc := svcURL.NewContainerURL(azureTestBucketName)
	if _, err = containerSvc.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone); err != nil {
		t.Fatalf("error while creating bucket: %v", err)
	}

	return func() { containerSvc.Delete(ctx, azblob.ContainerAccessConditions{}) }
}

func setupS3Bucket(t *testing.T) func() {
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

	return func() {
		client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(s3TestBucketName),
		})
	}
}

func TestVersion(t *testing.T) {
	old := files.Stdout
	buf := bytes.NewBuffer(nil)
	files.Stdout = buf
	defer func() { files.Stdout = old }()

	os.Args = []string{files.ProgramName, "version"}
	main()

	if !strings.Contains(buf.String(), fmt.Sprintf("Version:\tv%s", files.Version())) {
		t.Fatalf("expected version in version command output, did not recieve one:\n%s", buf.String())
	}

	buf.Reset()
	os.Args = []string{files.ProgramName, "version", "--jsonOutput"}
	main()
	var jout = struct {
		Version string
	}{}
	if err := json.Unmarshal(buf.Bytes(), &jout); err != nil {
		t.Fatalf("expected output to be JSON, got error while trying to decode - %v", err)
	} else if jout.Version != files.Version() {
		t.Fatalf("expected version to be '%s', got '%s' instead", files.Version(), jout.Version)
	}
}

func TestIntegration(t *testing.T) {
	removeAzureBucket := setupAzureBucket(t)
	defer removeAzureBucket()

	removeS3Bucket := setupS3Bucket(t)
	defer removeS3Bucket()

	s3bucket := fmt.Sprintf("%s://%s", backends.AWSS3BackendPrefix, s3TestBucketName)
	azurebucket := fmt.Sprintf("%s://%s", backends.AzureBackendPrefix, azureTestBucketName)
	bucket := fmt.Sprintf("%s,%s", s3bucket, azurebucket)

	// Azurite doesn't seem to like '|' so making seperator '-'
	// Backup Tests
	t.Run("Backup", func(t *testing.T) {
		// Manual Full Backup
		cmd.RootCmd.SetArgs([]string{"send", "--logLevel", logLevel, "--separator", "+", "tank/data@a", bucket})
		if err := cmd.RootCmd.Execute(); err != nil {
			t.Fatalf("error performing backup: %v", err)
		}

		cmd.ResetSendJobInfo()

		// Bookmark setup
		if err := exec.Command("zfs", "bookmark", "tank/data@a", "tank/data#a").Run(); err != nil {
			t.Fatalf("unexpected error creating bookmark tank/data#a: %v", err)
		}

		if err := exec.Command("zfs", "destroy", "tank/data@a").Run(); err != nil {
			t.Fatalf("unexpected error destroying snapshot tank/data@a: %v", err)
		}

		// Manual Incremental Backup from bookmark
		cmd.RootCmd.SetArgs([]string{"send", "--logLevel", logLevel, "--separator", "+", "-i", "tank/data#a", "tank/data@b", bucket})
		if err := cmd.RootCmd.Execute(); err != nil {
			t.Fatalf("error performing backup: %v", err)
		}

		cmd.ResetSendJobInfo()

		// Another Bookmark setup
		if err := exec.Command("zfs", "bookmark", "tank/data@b", "tank/data#b").Run(); err != nil {
			t.Fatalf("unexpected error creating bookmark tank/data#b: %v", err)
		}

		if err := exec.Command("zfs", "destroy", "tank/data@b").Run(); err != nil {
			t.Fatalf("unexpected error destroying snapshot tank/data@b: %v", err)
		}

		// "Smart" incremental Backup
		cmd.RootCmd.SetArgs([]string{"send", "--logLevel", logLevel, "--separator", "+", "--compressor", "xz", "--compressionLevel", "2", "--increment", "tank/data", bucket})
		if err := cmd.RootCmd.Execute(); err != nil {
			t.Fatalf("error performing backup: %v", err)
		}

		cmd.ResetSendJobInfo()

		// Smart Incremental Backup - Nothing to do
		cmd.RootCmd.SetArgs([]string{"send", "--logLevel", logLevel, "--separator", "+", "--increment", "tank/data", bucket})
		if err := cmd.RootCmd.Execute(); err != backup.ErrNoOp {
			t.Fatalf("expecting error %v, but got %v instead", backup.ErrNoOp, err)
		}

		cmd.ResetSendJobInfo()
	})

	var restoreTest = []struct {
		backend string
		bucket  string
		target  string
	}{
		{"AWSS3", s3bucket, "tank/data3"},
		{"Azure", azurebucket, "tank/data2"},
	}
	for _, test := range restoreTest {
		t.Run(fmt.Sprintf("List%s", test.backend), listWrapper(test.bucket))
		t.Run(fmt.Sprintf("Restore%s", test.backend), restoreWrapper(test.bucket, test.target))
	}
}

func listWrapper(bucket string) func(*testing.T) {
	return func(t *testing.T) {
		old := files.Stdout
		buf := bytes.NewBuffer(nil)
		files.Stdout = buf
		defer func() { files.Stdout = old }()

		var listTests = []struct {
			volumeName string
			after      time.Time
			before     time.Time
			keys       int
			entries    int
		}{
			// volumeName tests
			{"", time.Time{}, time.Time{}, 1, 3},
			{"t*", time.Time{}, time.Time{}, 1, 3},
			{"v*", time.Time{}, time.Time{}, 0, 0},
			{"tank/data", time.Time{}, time.Time{}, 1, 3},
			{"tan", time.Time{}, time.Time{}, 0, 0},
			//before Tests
			{"", time.Time{}, time.Now(), 1, 3},
			{"", time.Time{}, time.Now().Add(-24 * time.Hour), 0, 0},
			//after Tests
			{"", time.Now().Add(-24 * time.Hour), time.Time{}, 1, 3},
			{"", time.Now(), time.Time{}, 0, 0},
		}

		for _, test := range listTests {
			opts := []string{"list", "--logLevel", logLevel, "--jsonOutput"}
			if test.volumeName != "" {
				opts = append(opts, "--volumeName", test.volumeName)
			}
			if !test.after.IsZero() {
				opts = append(opts, "--after", test.after.Format(time.RFC3339[:19]))
			}
			if !test.before.IsZero() {
				opts = append(opts, "--before", test.before.Format(time.RFC3339[:19]))
			}

			cmd.RootCmd.SetArgs(append(opts, bucket))
			if err := cmd.RootCmd.Execute(); err != nil {
				t.Fatalf("error performing backup: %v", err)
			}
			cmd.ResetListJobInfo()

			jout := make(map[string][]*files.JobInfo)
			if err := json.Unmarshal(buf.Bytes(), &jout); err != nil {
				t.Fatalf("error parsing json output: %v", err)
			}

			if len(jout) != test.keys || len(jout["tank/data"]) != test.entries {
				t.Fatalf("expected %d keys and %d entries, got %d keys and %d entries", test.keys, test.entries, len(jout), len(jout["tank/data"]))
			}

			if len(jout["tank/data"]) == 3 {
				if jout["tank/data"][0].BaseSnapshot.Name != "a" || jout["tank/data"][1].BaseSnapshot.Name != "b" || jout["tank/data"][2].BaseSnapshot.Name != "c" {
					t.Fatalf("expected snapshot order a -> b -> c, but got %s -> %s -> %s instead", jout["tank/data"][0].BaseSnapshot.Name, jout["tank/data"][1].BaseSnapshot.Name, jout["tank/data"][2].BaseSnapshot.Name)
				}
			}

			buf.Reset()
		}
	}
}

func restoreWrapper(bucket, target string) func(*testing.T) {
	return func(t *testing.T) {
		cmd.ResetReceiveJobInfo()

		cmd.RootCmd.SetArgs([]string{"receive", "--logLevel", logLevel, "--separator", "+", "-F", "tank/data@a", bucket, target})
		if err := cmd.RootCmd.Execute(); err != nil {
			t.Fatalf("error performing receive: %v", err)
		}

		cmd.ResetReceiveJobInfo()

		cmd.RootCmd.SetArgs([]string{"receive", "--logLevel", logLevel, "--separator", "+", "-F", "-i", "tank/data@a", "tank/data@b", bucket, target})
		if err := cmd.RootCmd.Execute(); err != nil {
			t.Fatalf("error performing receive: %v", err)
		}

		cmd.ResetReceiveJobInfo()

		cmd.RootCmd.SetArgs([]string{"receive", "--logLevel", logLevel, "--separator", "+", "--workingDirectory", "./scratch", "-F", "--auto", "tank/data", bucket, target})
		if err := cmd.RootCmd.Execute(); err != nil {
			t.Fatalf("error performing receive: %v", err)
		}

		cmd.ResetReceiveJobInfo()

		diffCmd := exec.Command("diff", "-rq", "/tank/data", "/"+target)
		err := diffCmd.Run()
		if err != nil {
			t.Fatalf("unexpected difference comparing the restored backup %s: %v", target, err)
		}

		// Re-enable test when we get ZoL >= v0.7.0-rc5 on travis ci VM
		// cmd.RootCmd.SetArgs([]string{"receive", "--logLevel", logLevel, "--separator", "+", "-F", "--auto", "-o", "origin=tank/data@b", "tank/data", bucket, target+"origin"})
		// if err = cmd.RootCmd.Execute(); err != nil {
		// 	t.Fatalf("error performing receive: %v", err)
		// }

		// diffCmd = exec.Command("diff", "-rq", "/tank/data", target+"origin")
		// err = diffCmd.Run()
		// if err != nil {
		// 	t.Fatalf("unexpected difference comparing the restored backup %sorigin: %v", err, target)
		// }
	}
}

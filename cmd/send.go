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

package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/someone1/zfsbackup-go/backends"
	"github.com/someone1/zfsbackup-go/backup"
	"github.com/someone1/zfsbackup-go/helpers"
)

var (
	jobInfo         helpers.JobInfo
	fullIncremental string
	maxUploadSpeed  uint64
	passphrase      []byte
)

// sendCmd represents the send command
var sendCmd = &cobra.Command{
	Use:    "send [flags] filesystem|volume|snapshot uri(s)",
	Short:  "send will backup of a ZFS volume similar to how the \"zfs send\" command works.",
	Long:   `send take a subset of the`,
	PreRun: validateSendFlags,
	Run: func(cmd *cobra.Command, args []string) {
		updateJobInfo(args)

		helpers.AppLogger.Infof("Limiting the number of active files to %d", jobInfo.MaxFileBuffer)
		helpers.AppLogger.Infof("Limiting the number of parallel uploads to %d", jobInfo.MaxParallelUploads)
		helpers.AppLogger.Infof("Max Backoff Time will be %v", jobInfo.MaxBackoffTime)
		helpers.AppLogger.Infof("Max Upload Retry Time will be %v", jobInfo.MaxRetryTime)
		if jobInfo.EncryptKey != nil {
			helpers.AppLogger.Infof("Will be using encryption key for %s", jobInfo.EncryptTo)
		}

		if jobInfo.SignKey != nil {
			helpers.AppLogger.Infof("Will be signed from %s", jobInfo.SignFrom)
		}

		backup.Backup(&jobInfo)
	},
}

func init() {
	RootCmd.AddCommand(sendCmd)

	// ZFS send command options
	sendCmd.Flags().BoolVarP(&jobInfo.Replication, "replication", "R", false, "See the -R flag on zfs send for more information")
	sendCmd.Flags().BoolVarP(&jobInfo.Deduplication, "deduplication", "D", false, "See the -D flag for zfs send for more information.")
	sendCmd.Flags().StringVarP(&jobInfo.IncrementalSnapshot.Name, "incremental", "i", "", "See the -i flag on zfs send for more information")
	sendCmd.Flags().StringVarP(&fullIncremental, "intermediary", "I", "", "See the -I flag on zfs send for more information")
	sendCmd.Flags().BoolVarP(&jobInfo.Properties, "properties", "p", false, "See the -p flag on zfs send for more information.")

	// Specific to download only
	sendCmd.Flags().Uint64Var(&jobInfo.VolumeSize, "volsize", 200, "the maximum size (in MiB) a volume should be before splitting to a new volume. Note: zfsbackup will try its best to stay close/under this limit but it is not garaunteed.")
	sendCmd.Flags().IntVar(&jobInfo.CompressionLevel, "compressionLevel", 6, "the compression level to use with the compressor. Valid values are between 1-9.")
	sendCmd.Flags().BoolVar(&jobInfo.Resume, "resume", false, "set this flag to true when you want to try and resume a previously cancled or failed backup. It is up to the caller to ensure the same command line arguements are provided between the original backup and the resumed one.")
	sendCmd.Flags().BoolVar(&jobInfo.Full, "full", false, "set this flag to take a full backup of the specified volume using the most recent snapshot.")
	sendCmd.Flags().BoolVar(&jobInfo.Incremental, "increment", false, "set this flag to do an incremental backup of the most recent snapshot from the most recent snapshot found in the target.")
	sendCmd.Flags().DurationVar(&jobInfo.FullIfOlderThan, "fullIfOlderThan", -1*time.Minute, "set this flag to do an incremental backup of the most recent snapshot from the most recent snapshot found in the target unless the it's been greater than the time specified in this flag, then do a full backup.")

	sendCmd.Flags().IntVar(&jobInfo.MaxFileBuffer, "maxFileBuffer", 5, "the maximum number of files to have active during the upload process. Should be set to at least the number of max parallel uploads. Set to 0 to bypass local storage and upload straight to your destination - this will limit you to a single destination and disable any hash checks for the upload where available.")
	sendCmd.Flags().IntVar(&jobInfo.MaxParallelUploads, "maxParallelUploads", 4, "the maximum number of uploads to run in parallel.")
	sendCmd.Flags().Uint64Var(&maxUploadSpeed, "maxUploadSpeed", 0, "the maximum upload speed (in KB/s) the program should use between all upload workers. Use 0 for no limit")
	sendCmd.Flags().DurationVar(&jobInfo.MaxRetryTime, "maxRetryTime", 12*time.Hour, "the maximum time that can elapse when retrying a failed upload. Use 0 for no limit.")
	sendCmd.Flags().DurationVar(&jobInfo.MaxBackoffTime, "maxBackoffTime", 30*time.Minute, "the maximum delay you'd want a worker to sleep before retrying an upload.")
	sendCmd.Flags().StringVar(&jobInfo.Separator, "separator", "|", "the separator to use between object component names.")
}

func updateJobInfo(args []string) {
	jobInfo.StartTime = time.Now()
	jobInfo.Version = helpers.VersionNumber

	if fullIncremental != "" {
		jobInfo.IncrementalSnapshot.Name = fullIncremental
		jobInfo.IntermediaryIncremental = true
	}

	parts := strings.Split(args[0], "@")
	jobInfo.VolumeName = parts[0]
	jobInfo.Destinations = strings.Split(args[1], ",")

	if len(jobInfo.Destinations) > 1 && jobInfo.MaxFileBuffer == 0 {
		helpers.AppLogger.Errorf("Specifying multiple destinations and a MaxFileBuffer size of 0 is unsupported.")
		panic(helpers.Exit{Code: 10})
	}

	for _, destination := range jobInfo.Destinations {
		prefix := strings.Split(destination, "://")
		if len(prefix) < 2 {
			helpers.AppLogger.Errorf("Invalid destination URI provided: %s.", destination)
			panic(helpers.Exit{Code: 10})
		}
		_, err := backends.GetBackendForPrefix(prefix[0])
		if err == backends.ErrInvalidPrefix {
			helpers.AppLogger.Errorf("Unsupported prefix provided in destination URI, was given %s", prefix[0])
			panic(helpers.Exit{Code: 10})
		}
	}

	// If we aren't using a "smart" option, rely on the user to provide the snapshots to use!
	if !jobInfo.Full && !jobInfo.Incremental && jobInfo.FullIfOlderThan == -1*time.Minute {
		if len(parts) != 2 {
			helpers.AppLogger.Errorf("Invalid base snapshot provided. Expected format <volume>@<snapshot>, got %s instead", args[0])
			panic(helpers.Exit{Code: 10})
		}
		jobInfo.BaseSnapshot = helpers.SnapshotInfo{Name: parts[1]}
		creationTime, err := helpers.GetCreationDate(context.TODO(), args[0])
		if err != nil {
			helpers.AppLogger.Errorf("Error trying to get creation date of specified base snapshot - %v", err)
			panic(helpers.Exit{Code: 10})
		}
		jobInfo.BaseSnapshot.CreationTime = creationTime

		if jobInfo.IncrementalSnapshot.Name != "" {
			jobInfo.IncrementalSnapshot.Name = strings.TrimPrefix(jobInfo.IncrementalSnapshot.Name, jobInfo.VolumeName)
			jobInfo.IncrementalSnapshot.Name = strings.TrimPrefix(jobInfo.IncrementalSnapshot.Name, "@")

			creationTime, err = helpers.GetCreationDate(context.TODO(), fmt.Sprintf("%s@%s", jobInfo.VolumeName, jobInfo.IncrementalSnapshot.Name))
			if err != nil {
				helpers.AppLogger.Errorf("Error trying to get creation date of specified incremental snapshot - %v", err)
				panic(helpers.Exit{Code: 10})
			}
			jobInfo.IncrementalSnapshot.CreationTime = creationTime
		}
	} else {
		// Some basic checks here
		onlyOneCheck := 0
		if jobInfo.Full {
			onlyOneCheck++
		}
		if jobInfo.Incremental {
			onlyOneCheck++
		}
		if jobInfo.FullIfOlderThan != -1*time.Minute {
			onlyOneCheck++
		}
		if onlyOneCheck > 1 {
			helpers.AppLogger.Errorf("Please specify only one \"smart\" option at a time")
			panic(helpers.Exit{Code: 11})
		}
		if len(parts) != 1 {
			helpers.AppLogger.Errorf("When using a smart option, please only specify the volume to backup, do not include any snapshot information.")
			panic(helpers.Exit{Code: 10})
		}
		if err := backup.ProcessSmartOptions(&jobInfo); err != nil {
			helpers.AppLogger.Errorf("Error while trying to process smart option - %v", err)
			panic(helpers.Exit{Code: 10})
		}
		helpers.AppLogger.Debugf("Utilizing smart option.")
	}
}

func validateSendFlags(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		panic(helpers.Exit{Code: 10})
	}

	if jobInfo.IncrementalSnapshot.Name != "" && fullIncremental != "" {
		helpers.AppLogger.Errorf("The flags -i and -I are mutually exclusive. Please specify only one of these flags.")
		panic(helpers.Exit{Code: 10})
	}

	if err := jobInfo.ValidateSendFlags(); err != nil {
		helpers.AppLogger.Error(err)
		panic(helpers.Exit{Code: 10})
	}
}

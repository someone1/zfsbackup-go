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
	"github.com/someone1/zfsbackup-go/config"
	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
	"github.com/someone1/zfsbackup-go/zfs"
)

var (
	jobInfo         files.JobInfo
	fullIncremental string
	maxUploadSpeed  uint64
	passphrase      []byte
)

// sendCmd represents the send command
var sendCmd = &cobra.Command{
	Use:     "send [flags] filesystem|volume|snapshot uri(s)",
	Short:   "send will backup of a ZFS volume similar to how the \"zfs send\" command works.",
	Long:    `send take a subset of the`,
	PreRunE: validateSendFlags,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.AppLogger.Infof("Limiting the number of active files to %d", jobInfo.MaxFileBuffer)
		log.AppLogger.Infof("Limiting the number of parallel uploads to %d", jobInfo.MaxParallelUploads)
		log.AppLogger.Infof("Max Backoff Time will be %v", jobInfo.MaxBackoffTime)
		log.AppLogger.Infof("Max Upload Retry Time will be %v", jobInfo.MaxRetryTime)
		log.AppLogger.Infof("Upload Chunk Size will be %dMiB", jobInfo.UploadChunkSize)
		if jobInfo.EncryptKey != nil {
			log.AppLogger.Infof("Will be using encryption key for %s", jobInfo.EncryptTo)
		}

		if jobInfo.SignKey != nil {
			log.AppLogger.Infof("Will be signed from %s", jobInfo.SignFrom)
		}

		return backup.Backup(cmd.Context(), &jobInfo)
	},
}

// nolint:funlen,gocyclo // Will do later
func init() {
	RootCmd.AddCommand(sendCmd)

	// ZFS send command options
	sendCmd.Flags().BoolVarP(&jobInfo.Replication, "replication", "R", false, "See the -R flag on zfs send for more information")
	sendCmd.Flags().BoolVarP(&jobInfo.Deduplication, "deduplication", "D", false, "See the -D flag for zfs send for more information.")
	sendCmd.Flags().StringVarP(&jobInfo.IncrementalSnapshot.Name, "incremental", "i", "", "See the -i flag on zfs send for more information")
	sendCmd.Flags().StringVarP(&fullIncremental, "intermediary", "I", "", "See the -I flag on zfs send for more information")
	sendCmd.Flags().BoolVarP(&jobInfo.Properties, "properties", "p", false, "See the -p flag on zfs send for more information.")

	// Specific to download only
	sendCmd.Flags().Uint64Var(
		&jobInfo.VolumeSize,
		"volsize",
		200,
		"the maximum size (in MiB) a volume should be before splitting to a new volume. Note: zfsbackup will try its best to stay close/under "+
			"this limit but it is not guaranteed.",
	)
	sendCmd.Flags().IntVar(
		&jobInfo.CompressionLevel,
		"compressionLevel",
		6,
		"the compression level to use with the compressor. Valid values are between 1-9.",
	)
	sendCmd.Flags().BoolVar(
		&jobInfo.Resume,
		"resume",
		false,
		"set this flag to true when you want to try and resume a previously cancled or failed backup. It is up to the caller to ensure the same "+
			"command line arguments are provided between the original backup and the resumed one.",
	)
	sendCmd.Flags().BoolVar(
		&jobInfo.Full,
		"full",
		false,
		"set this flag to take a full backup of the specified volume using the most recent snapshot.",
	)
	sendCmd.Flags().BoolVar(
		&jobInfo.Incremental,
		"increment",
		false,
		"set this flag to do an incremental backup of the most recent snapshot from the most recent snapshot found in the target.",
	)
	sendCmd.Flags().StringVar(
		&jobInfo.SnapshotPrefix,
		"snapshotPrefix",
		"",
		"Only consider snapshots starting with the given snapshot prefix",
	)
	sendCmd.Flags().DurationVar(
		&jobInfo.FullIfOlderThan,
		"fullIfOlderThan",
		-1*time.Minute,
		"set this flag to do an incremental backup of the most recent snapshot from the most recent snapshot found in the target unless the "+
			"it's been greater than the time specified in this flag, then do a full backup.",
	)
	sendCmd.Flags().StringVar(
		&jobInfo.Compressor,
		"compressor",
		files.InternalCompressor,
		"specify to use the internal (parallel) gzip implementation or an external binary (e.g. gzip, bzip2, pigz, lzma, xz, etc.) Syntax "+
			"must be similar to the gzip compression tool) to compress the stream for storage. Please take into consideration time, memory, "+
			"and CPU usage for any of the compressors used. All manifests utilize the internal compressor. If value is zfs, the zfs stream "+
			"will be created compressed. See the -c flag on zfs send for more information.",
	)
	sendCmd.Flags().IntVar(
		&jobInfo.MaxFileBuffer,
		"maxFileBuffer",
		5,
		"the maximum number of files to have active during the upload process. Should be set to at least the number of max parallel uploads. "+
			"Set to 0 to bypass local storage and upload straight to your destination - this will limit you to a single destination and disable "+
			"any hash checks for the upload where available.",
	)
	sendCmd.Flags().IntVar(
		&jobInfo.MaxParallelUploads,
		"maxParallelUploads",
		4,
		"the maximum number of uploads to run in parallel.",
	)
	sendCmd.Flags().Uint64Var(
		&maxUploadSpeed,
		"maxUploadSpeed",
		0,
		"the maximum upload speed (in KB/s) the program should use between all upload workers. Use 0 for no limit",
	)
	sendCmd.Flags().DurationVar(
		&jobInfo.MaxRetryTime,
		"maxRetryTime",
		12*time.Hour,
		"the maximum time that can elapse when retrying a failed upload. Use 0 for no limit.",
	)
	sendCmd.Flags().DurationVar(
		&jobInfo.MaxBackoffTime,
		"maxBackoffTime",
		30*time.Minute,
		"the maximum delay you'd want a worker to sleep before retrying an upload.",
	)
	sendCmd.Flags().StringVar(
		&jobInfo.Separator,
		"separator",
		"|",
		"the separator to use between object component names.",
	)
	sendCmd.Flags().IntVar(
		&jobInfo.UploadChunkSize,
		"uploadChunkSize",
		10,
		"the chunk size, in MiB, to use when uploading. A minimum of 5MiB and maximum of 100MiB is enforced.",
	)
	sendCmd.Flags().BoolVar(
		&jobInfo.SmartIntermediaryIncremental,
		"smartIntermediaryIncremental",
		false,
		"store intermediary snapshots when using smart options",
	)
}

// ResetSendJobInfo exists solely for integration testing
func ResetSendJobInfo() {
	resetRootFlags()
	// ZFS send command options
	jobInfo.Replication = false
	jobInfo.Deduplication = false
	jobInfo.IncrementalSnapshot = files.SnapshotInfo{}
	jobInfo.BaseSnapshot = files.SnapshotInfo{}
	fullIncremental = ""
	jobInfo.Properties = false

	// Specific to download only
	jobInfo.VolumeSize = 200
	jobInfo.CompressionLevel = 6
	jobInfo.Resume = false
	jobInfo.Full = false
	jobInfo.Incremental = false
	jobInfo.FullIfOlderThan = -1 * time.Minute

	jobInfo.MaxFileBuffer = 5
	jobInfo.MaxParallelUploads = 4
	maxUploadSpeed = 0
	jobInfo.MaxRetryTime = 12 * time.Hour
	jobInfo.MaxBackoffTime = 30 * time.Minute
	jobInfo.Separator = "|"
	jobInfo.UploadChunkSize = 10
	jobInfo.Compressor = files.InternalCompressor
}

// nolint:gocyclo,funlen // Will do later
func updateJobInfo(args []string) error {
	jobInfo.StartTime = time.Now()
	jobInfo.Version = config.VersionNumber

	if fullIncremental != "" {
		jobInfo.IncrementalSnapshot.Name = fullIncremental
		jobInfo.IntermediaryIncremental = true
	}

	parts := strings.Split(args[0], "@")
	jobInfo.VolumeName = parts[0]
	jobInfo.Destinations = strings.Split(args[1], ",")

	if len(jobInfo.Destinations) > 1 && jobInfo.MaxFileBuffer == 0 {
		log.AppLogger.Errorf("Specifying multiple destinations and a MaxFileBuffer size of 0 is unsupported.")
		return errInvalidInput
	}

	for _, destination := range jobInfo.Destinations {
		_, err := backends.GetBackendForURI(destination)
		if err == backends.ErrInvalidPrefix {
			log.AppLogger.Errorf("Unsupported prefix provided in destination URI, was given %s", destination)
			return err
		} else if err == backends.ErrInvalidURI {
			log.AppLogger.Errorf("Unsupported destination URI, was given %s", destination)
			return err
		}
	}

	// If we aren't using a "smart" option, rely on the user to provide the snapshots to use!
	if !usingSmartOption() {
		if len(parts) != 2 {
			log.AppLogger.Errorf("Invalid base snapshot provided. Expected format <volume>@<snapshot>, got %s instead", args[0])
			return errInvalidInput
		}
		jobInfo.BaseSnapshot = files.SnapshotInfo{Name: parts[1]}
		creationTime, err := zfs.GetCreationDate(context.TODO(), args[0])
		if err != nil {
			log.AppLogger.Errorf("Error trying to get creation date of specified base snapshot - %v", err)
			return err
		}
		jobInfo.BaseSnapshot.CreationTime = creationTime

		if jobInfo.IncrementalSnapshot.Name != "" {
			var targetName string
			jobInfo.IncrementalSnapshot.Name = strings.TrimPrefix(jobInfo.IncrementalSnapshot.Name, jobInfo.VolumeName)
			if strings.HasPrefix(jobInfo.IncrementalSnapshot.Name, "#") {
				jobInfo.IncrementalSnapshot.Name = strings.TrimPrefix(jobInfo.IncrementalSnapshot.Name, "#")
				targetName = fmt.Sprintf("%s#%s", jobInfo.VolumeName, jobInfo.IncrementalSnapshot.Name)
				jobInfo.IncrementalSnapshot.Bookmark = true
			} else {
				jobInfo.IncrementalSnapshot.Name = strings.TrimPrefix(jobInfo.IncrementalSnapshot.Name, "@")
				targetName = fmt.Sprintf("%s@%s", jobInfo.VolumeName, jobInfo.IncrementalSnapshot.Name)
			}

			creationTime, err = zfs.GetCreationDate(context.TODO(), targetName)
			if err != nil {
				log.AppLogger.Errorf("Error trying to get creation date of specified incremental snapshot/bookmark - %v", err)
				return err
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
			log.AppLogger.Errorf("Please specify only one \"smart\" option at a time")
			return errInvalidInput
		}
		if len(parts) != 1 {
			log.AppLogger.Errorf("When using a smart option, please only specify the volume to backup, do not include any snapshot information.")
			return errInvalidInput
		}
		if err := backup.ProcessSmartOptions(context.Background(), &jobInfo); err != nil {
			log.AppLogger.Errorf("Error while trying to process smart option - %v", err)
			return err
		}
		log.AppLogger.Debugf("Utilizing smart option.")
	}

	return nil
}

func usingSmartOption() bool {
	return jobInfo.Full || jobInfo.Incremental || jobInfo.FullIfOlderThan != -1*time.Minute
}

func validateSendFlags(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		_ = cmd.Usage()
		return errInvalidInput
	}

	if err := loadSendKeys(); err != nil {
		return err
	}

	if jobInfo.IncrementalSnapshot.Name != "" && fullIncremental != "" {
		log.AppLogger.Errorf("The flags -i and -I are mutually exclusive. Please specify only one of these flags.")
		return errInvalidInput
	}

	if err := jobInfo.ValidateSendFlags(); err != nil {
		log.AppLogger.Error(err)
		return err
	}

	return updateJobInfo(args)
}

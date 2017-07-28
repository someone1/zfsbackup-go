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
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/someone1/zfsbackup-go/backends"
	"github.com/someone1/zfsbackup-go/backup"
	"github.com/someone1/zfsbackup-go/helpers"
)

// receiveCmd represents the receive command
var receiveCmd = &cobra.Command{
	Use:     "receive [flags] filesystem|volume|snapshot uri local_volume",
	Short:   "receive will restore a snapshot of a ZFS volume similar to how the \"zfs recv\" command works.",
	Long:    `receive will restore a snapshot of a ZFS volume similar to how the "zfs recv" command works.`,
	PreRunE: validateReceiveFlags,
	RunE: func(cmd *cobra.Command, args []string) error {
		return backup.Receive(context.Background(), &jobInfo)
	},
}

func init() {
	RootCmd.AddCommand(receiveCmd)

	// ZFS recv command options
	receiveCmd.Flags().BoolVarP(&jobInfo.FullPath, "fullPath", "d", false, "See the -d flag on zfs recv for more information")
	receiveCmd.Flags().BoolVarP(&jobInfo.LastPath, "lastPath", "e", false, "See the -e flag for zfs recv for more information.")
	receiveCmd.Flags().BoolVarP(&jobInfo.Force, "force", "F", false, "See the -F flag for zfs recv for more information.")
	receiveCmd.Flags().BoolVarP(&jobInfo.NotMounted, "unmounted", "u", false, "See the -u flag for zfs recv for more information.")
	receiveCmd.Flags().StringVarP(&jobInfo.Origin, "origin", "o", "", "See the -o flag on zfs recv for more information.")
	receiveCmd.Flags().StringVarP(&jobInfo.IncrementalSnapshot.Name, "incremental", "i", "", "Used to specify the snapshot target to restore from.")
	receiveCmd.Flags().IntVar(&jobInfo.MaxFileBuffer, "maxFileBuffer", 5, "the maximum number of files to have active during the upload process. Should be set to at least the number of max parallel uploads. Set to 0 to bypass local storage and upload straight to your destination - this will limit you to a single destination and disable any hash checks for the upload where available.")
	receiveCmd.Flags().DurationVar(&jobInfo.MaxRetryTime, "maxRetryTime", 12*time.Hour, "the maximum time that can elapse when retrying a failed download. Use 0 for no limit.")
	receiveCmd.Flags().DurationVar(&jobInfo.MaxBackoffTime, "maxBackoffTime", 30*time.Minute, "the maximum delay you'd want a worker to sleep before retrying an download.")
}

func validateReceiveFlags(cmd *cobra.Command, args []string) error {
	if len(args) != 3 {
		cmd.Usage()
		return errInvalidInput
	}
	jobInfo.StartTime = time.Now()

	parts := strings.Split(args[0], "@")
	if len(parts) != 2 {
		helpers.AppLogger.Errorf("Invalid base snapshot provided. Expected format <volume>@<snapshot>, got %s instead", args[0])
		return errInvalidInput
	}
	jobInfo.VolumeName = parts[0]
	jobInfo.BaseSnapshot = helpers.SnapshotInfo{Name: parts[1]}
	jobInfo.Destinations = strings.Split(args[1], ",")
	jobInfo.LocalVolume = args[2]

	if jobInfo.IncrementalSnapshot.Name != "" {
		jobInfo.IncrementalSnapshot.Name = strings.TrimPrefix(jobInfo.IncrementalSnapshot.Name, jobInfo.VolumeName)
		jobInfo.IncrementalSnapshot.Name = strings.TrimPrefix(jobInfo.IncrementalSnapshot.Name, "@")
	}

	for _, destination := range jobInfo.Destinations {
		_, err := backends.GetBackendForURI(destination)
		if err == backends.ErrInvalidPrefix {
			helpers.AppLogger.Errorf("Unsupported prefix provided in destination URI, was given %s", destination)
			return errInvalidInput
		} else if err == backends.ErrInvalidURI {
			helpers.AppLogger.Errorf("Invalid destination URI, was given %s", destination)
			return errInvalidInput
		}
	}

	return nil
}

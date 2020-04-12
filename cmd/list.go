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
	"time"

	"github.com/spf13/cobra"

	"github.com/someone1/zfsbackup-go/backup"
	"github.com/someone1/zfsbackup-go/log"
)

var (
	startsWith string
	beforeStr  string
	afterStr   string
	before     time.Time
	after      time.Time
)

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:     "list [flags] uri",
	Short:   "List all backup sets found at the provided target.",
	Long:    `List all backup sets found at the provided target.`,
	PreRunE: validateListFlags,
	RunE: func(cmd *cobra.Command, args []string) error {
		if startsWith != "" {
			if startsWith[len(startsWith)-1:] == "*" {
				log.AppLogger.Infof("Listing all backup jobs for volumes starting with %s", startsWith)
			} else {
				log.AppLogger.Infof("Listing all backup jobs for volume %s", startsWith)
			}
		}

		if !before.IsZero() {
			log.AppLogger.Infof("Listing all back jobs of snapshots taken before %v", before)
		}

		if !after.IsZero() {
			log.AppLogger.Infof("Listing all back jobs of snapshots taken after %v", after)
		}

		jobInfo.Destinations = []string{args[0]}
		return backup.List(context.Background(), &jobInfo, startsWith, before, after)
	},
}

func init() {
	RootCmd.AddCommand(listCmd)

	listCmd.Flags().StringVar(&startsWith, "volumeName", "", "Filter results to only this volume name, can end with a '*' to match as only a prefix")
	listCmd.Flags().StringVar(&beforeStr, "before", "", "Filter results to only this backups before this specified date & time (format: yyyy-MM-ddTHH:mm:ss, parsed in local TZ)")
	listCmd.Flags().StringVar(&afterStr, "after", "", "Filter results to only this backups after this specified date & time (format: yyyy-MM-ddTHH:mm:ss, parsed in local TZ)")
}

func validateListFlags(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		cmd.Usage()
		return errInvalidInput
	}

	if beforeStr != "" {
		parsed, perr := time.ParseInLocation(time.RFC3339[:19], beforeStr, time.Local)
		if perr != nil {
			log.AppLogger.Errorf("could not parse before time '%s' due to error: %v", beforeStr, perr)
			return perr
		}
		before = parsed
	}

	if afterStr != "" {
		parsed, perr := time.ParseInLocation(time.RFC3339[:19], afterStr, time.Local)
		if perr != nil {
			log.AppLogger.Errorf("could not parse before time '%s' due to error: %v", beforeStr, perr)
			return perr
		}
		after = parsed
	}
	return nil
}

// ResetListJobInfo exists solely for integration testing
func ResetListJobInfo() {
	resetRootFlags()
	startsWith = ""
	beforeStr = ""
	afterStr = ""
	before = time.Time{}
	after = time.Time{}
}

// Copyright © 2017 Prateek Malhotra (someone1@gmail.com)
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

	"github.com/spf13/cobra"

	"github.com/someone1/zfsbackup-go/backup"
)

var cleanLocal bool

// cleanCmd represents the clean command
var cleanCmd = &cobra.Command{
	Use:           "clean [flags] uri",
	Short:         "Clean will delete any objects in the target that are not found in the manifest files found in the target.",
	Long:          `Clean will delete any objects in the target that are not found in the manifest files found in the target.`,
	SilenceErrors: true,
	PreRunE:       validateCleanFlags,
	RunE: func(cmd *cobra.Command, args []string) error {
		jobInfo.Destinations = []string{args[0]}
		return backup.Clean(context.Background(), &jobInfo, cleanLocal)
	},
}

func init() {
	RootCmd.AddCommand(cleanCmd)

	cleanCmd.Flags().BoolVarP(&cleanLocal, "cleanLocal", "", false, "Delete any files found in the local cache that shouldn't be there.")
	cleanCmd.Flags().BoolVarP(&jobInfo.Force, "force", "", false,
		"This will force the deletion of broken backup sets (sets where volumes expected in the manifest file are not found). Use with caution.",
	)
}

func validateCleanFlags(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		_ = cmd.Usage()
		return errInvalidInput
	}

	if err := loadReceiveKeys(); err != nil {
		return err
	}

	return nil
}

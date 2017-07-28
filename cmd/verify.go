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

// verifyCmd represents the verify command
var verifyCmd = &cobra.Command{
	Use:     "verify filesystem|volume|snapshot uri",
	Short:   "Verify will ensure that the backupset for the given snapshot exists in the target",
	Long:    `Verify will ensure that the backupset for the given snapshot exists in the target`,
	PreRunE: validateVerifyFlags,
	RunE: func(cmd *cobra.Command, args []string) error {
		return backup.Verify(context.Background(), &jobInfo)
	},
}

func init() {
	RootCmd.AddCommand(verifyCmd)
}

func validateVerifyFlags(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		cmd.Usage()
		return errInvalidInput
	}
	return updateJobInfo(args)
}

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
	"fmt"
	"runtime"


	"github.com/someone1/zfsbackup-go/helpers"

	"github.com/spf13/cobra"
)

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of zfsbackup in use and relevant compile information",
	Long: `This will output the version of zfsbackup in use and information about
the runtime and architecture.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("\tProgram Name:\t%s\n\tVersion:\tv%s\n\tOS Target:\t%s\n\tArch Target:\t%s\n\tCompiled With:\t%s\n\tGo Version:\t%s\n", helpers.ProgramName, helpers.Version(), runtime.GOOS, runtime.GOARCH, runtime.Compiler, runtime.Version())
		return nil
	},
}

func init() {
	RootCmd.AddCommand(versionCmd)
}

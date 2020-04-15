package config

import (
	"io"
	"os"

	"github.com/juju/ratelimit"
)

var (
	// Stdout is where to output standard messaging to
	Stdout io.Writer = os.Stdout
	// JSONOutput will signal if we should dump the results to Stdout JSON formatted
	JSONOutput = false
	// BackupUploadBucket is the bandwidth rate-limit bucket if we need one.
	BackupUploadBucket *ratelimit.Bucket
	// BackupTempdir is the scratch space for our output
	BackupTempdir string
	// WorkingDir is the directory that all the cache/scratch work is done for this program
	WorkingDir string
)

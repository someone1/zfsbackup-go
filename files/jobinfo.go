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

package files

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	"golang.org/x/crypto/openpgp"

	"github.com/someone1/zfsbackup-go/log"
)

var (
	disallowedSeps = regexp.MustCompile(`^[\w\-:\.]+`) // Disallowed by ZFS
)

// JobInfo represents the relevant information for a job that can be used to read
// in details of that job at a later time.
type JobInfo struct {
	StartTime               time.Time
	EndTime                 time.Time
	VolumeName              string
	BaseSnapshot            SnapshotInfo
	IncrementalSnapshot     SnapshotInfo
	Compressor              string
	CompressionLevel        int
	Separator               string
	ZFSCommandLine          string
	ZFSStreamBytes          uint64
	Volumes                 []*VolumeInfo
	Version                 float64
	EncryptTo               string
	SignFrom                string
	Replication             bool
	Deduplication           bool
	Properties              bool
	IntermediaryIncremental bool
	Resume                  bool `json:"-"`
	// "Smart" Options
	Full            bool          `json:"-"`
	Incremental     bool          `json:"-"`
	FullIfOlderThan time.Duration `json:"-"`

	// ZFS Receive options
	Force       bool   `json:"-"`
	FullPath    bool   `json:"-"`
	LastPath    bool   `json:"-"`
	NotMounted  bool   `json:"-"`
	Origin      string `json:"-"`
	LocalVolume string `json:"-"`
	AutoRestore bool   `json:"-"`

	Destinations       []string        `json:"-"`
	VolumeSize         uint64          `json:"-"`
	ManifestPrefix     string          `json:"-"`
	MaxBackoffTime     time.Duration   `json:"-"`
	MaxRetryTime       time.Duration   `json:"-"`
	MaxParallelUploads int             `json:"-"`
	MaxFileBuffer      int             `json:"-"`
	EncryptKey         *openpgp.Entity `json:"-"`
	SignKey            *openpgp.Entity `json:"-"`
	ParentSnap         *JobInfo        `json:"-"`
	UploadChunkSize    int             `json:"-"`
}

// SnapshotInfo represents a snapshot with relevant information.
type SnapshotInfo struct {
	CreationTime time.Time
	Name         string
	Bookmark     bool
}

// Equal will test two SnapshotInfo objects for equality. This is based on the snapshot name and the time of creation
func (s *SnapshotInfo) Equal(t *SnapshotInfo) bool {
	if s == nil || t == nil {
		return s == t
	}
	return strings.Compare(s.Name, t.Name) == 0 && s.CreationTime.Equal(t.CreationTime)
}

// TotalBytesWritten will sum up the size of all underlying Volumes to give a total
// that represents how many bytes have been written.
func (j *JobInfo) TotalBytesWritten() uint64 {
	var total uint64

	for _, vol := range j.Volumes {
		total += vol.Size
	}

	return total
}

// String will return a string representation of this JobInfo.
func (j *JobInfo) String() string {
	var output []string
	output = append(output, fmt.Sprintf("Volume: %s", j.VolumeName))
	output = append(output, fmt.Sprintf("Snapshot: %s (%v)", j.BaseSnapshot.Name, j.BaseSnapshot.CreationTime))
	if j.IncrementalSnapshot.Name != "" {
		output = append(output, fmt.Sprintf("Incremental From Snapshot: %s (%v)", j.IncrementalSnapshot.Name, j.IncrementalSnapshot.CreationTime))
		output = append(output, fmt.Sprintf("Intermediary: %v", j.IntermediaryIncremental))
	}
	output = append(output, fmt.Sprintf("Replication: %v", j.Replication))
	totalWrittenBytes := j.TotalBytesWritten()
	output = append(output, fmt.Sprintf("Archives: %d - %d bytes (%s)", len(j.Volumes), totalWrittenBytes, humanize.IBytes(totalWrittenBytes)))
	output = append(output, fmt.Sprintf("Volume Size (Raw): %d bytes (%s)", j.ZFSStreamBytes, humanize.IBytes(j.ZFSStreamBytes)))
	output = append(output, fmt.Sprintf("Uploaded: %v (took %v)\n\n", j.StartTime, j.EndTime.Sub(j.StartTime)))
	return strings.Join(output, "\n\t")
}

// TotalBytesStreamedAndVols will sum up the streamed bytes of all underlying Volumes to give a total
// that represents how many bytes have been streamed. It will stop at any out of order volume number.
func (j *JobInfo) TotalBytesStreamedAndVols() (total uint64, volnum int64) {
	volnum = 1
	if len(j.Volumes) > 0 {
		for _, vol := range j.Volumes {
			if vol.VolumeNumber != volnum {
				break
			}
			total += vol.ZFSStreamBytes
			volnum++
		}
		j.Volumes = j.Volumes[:volnum-1]
	}
	return
}

// ValidateSendFlags will check if the options assigned to this JobInfo object is
// properly within the bounds for a send backup operation.
func (j *JobInfo) ValidateSendFlags() error {
	if j.MaxFileBuffer < 0 {
		return fmt.Errorf("The number of active files must be set to a value greater than or equal to 0. Was given %d", j.MaxFileBuffer)
	}

	if j.MaxParallelUploads <= 0 {
		return fmt.Errorf("The number of parallel uploads must be set to a value greater than 0. Was given %d", j.MaxParallelUploads)
	}

	if j.MaxFileBuffer < j.MaxParallelUploads {
		log.AppLogger.Warningf("The number of parallel uploads (%d) is greater than the number of active files allowed (%d), this may result in an unachievable max parallel upload target.", j.MaxParallelUploads, j.MaxFileBuffer)
	}

	if j.MaxRetryTime < 0 {
		return fmt.Errorf("The max retry time must be set to a value greater than or equal to 0. Was given %d", j.MaxRetryTime)
	}

	if j.MaxBackoffTime <= 0 {
		return fmt.Errorf("The max backoff time must be set to a value greater than 0. Was given %d", j.MaxBackoffTime)
	}

	if j.CompressionLevel < 1 || j.CompressionLevel > 9 {
		return fmt.Errorf("The compression level specified must be between 1 and 9. Was given %d", j.CompressionLevel)
	}

	if disallowedSeps.MatchString(j.Separator) {
		return fmt.Errorf("The separator provided (%s) should not be used as it can conflict with allowed characters in zfs components", j.Separator)
	}

	if j.UploadChunkSize < 5 || j.UploadChunkSize > 100 {
		return fmt.Errorf("The uploadChunkSize provided (%d) is not between 5 and 100", j.UploadChunkSize)
	}

	return nil
}

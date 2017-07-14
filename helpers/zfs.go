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

package helpers

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// GetCreationDate will use the zfs command to get and parse the creation datetime
// of the specified volume/snapshot
func GetCreationDate(ctx context.Context, target string) (time.Time, error) {
	rawTime, err := GetZFSProperty(ctx, "creation", target)
	if err != nil {
		return time.Time{}, err
	}
	epochTime, serr := strconv.ParseInt(rawTime, 10, 64)
	if serr != nil {
		return time.Time{}, serr
	}
	return time.Unix(epochTime, 0), nil
}

// GetZFSProperty will return the raw value returned by the "zfs get" command for
// the given property on the given target.
func GetZFSProperty(ctx context.Context, prop, target string) (string, error) {
	b := new(bytes.Buffer)
	errB := new(bytes.Buffer)
	cmd := exec.CommandContext(ctx, "zfs", "get", "-H", "-p", "-o", "value", prop, target)
	AppLogger.Debugf("Getting ZFS Property with command \"%s\"", strings.Join(cmd.Args, " "))
	cmd.Stdout = b
	cmd.Stderr = errB
	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("%s (%v)", strings.TrimSpace(errB.String()), err)
	}
	return strings.TrimSpace(b.String()), nil
}

// GetZFSSendCommand will return the send command to use for the given JobInfo
func GetZFSSendCommand(ctx context.Context, j *JobInfo) *exec.Cmd {

	// Prepare the zfs send command
	zfsArgs := []string{"send"}

	if j.Replication {
		AppLogger.Infof("Enabling the replication (-R) flag on the send.")
		zfsArgs = append(zfsArgs, "-R")
	}

	if j.Deduplication {
		AppLogger.Infof("Enabling the deduplication (-D) flag on the send.")
		zfsArgs = append(zfsArgs, "-D")
	}

	if j.Properties {
		AppLogger.Infof("Enabling the properties (-p) flag on the send.")
		zfsArgs = append(zfsArgs, "-p")
	}

	if j.IntermediaryIncremental {
		AppLogger.Infof("Enabling an incremental stream with all intermediary snapshots (-I) on the send to snapshot %s", j.IncrementalSnapshot.Name)
		zfsArgs = append(zfsArgs, "-I", j.IncrementalSnapshot.Name)
	}

	if j.IncrementalSnapshot.Name != "" {
		AppLogger.Infof("Enabling an incremental stream (-i) on the send to snapshot %s", j.IncrementalSnapshot.Name)
		zfsArgs = append(zfsArgs, "-i", j.IncrementalSnapshot.Name)
	}

	zfsArgs = append(zfsArgs, fmt.Sprintf("%s@%s", j.VolumeName, j.BaseSnapshot.Name))
	cmd := exec.CommandContext(ctx, "zfs", zfsArgs...)

	return cmd
}

// GetZFSReceiveCommand will return the recv command to use for the given JobInfo
func GetZFSReceiveCommand(ctx context.Context, j *JobInfo) *exec.Cmd {

	// Prepare the zfs send command
	zfsArgs := []string{"receive"}

	if j.FullPath {
		AppLogger.Infof("Enabling the full path (-d) flag on the receive.")
		zfsArgs = append(zfsArgs, "-d")
	}

	if j.LastPath {
		AppLogger.Infof("Enabling the last path (-e) flag on the receive.")
		zfsArgs = append(zfsArgs, "-e")
	}

	if j.NotMounted {
		AppLogger.Infof("Enabling the not mounted (-u) flag on the receive.")
		zfsArgs = append(zfsArgs, "-u")
	}

	if j.Force {
		AppLogger.Infof("Enabling the forced rollback (-F) flag on the receive.")
		zfsArgs = append(zfsArgs, "-F")
	}

	if j.Origin != "" {
		AppLogger.Infof("Enabling the origin flag (-o) on the receive to %s", j.Origin)
		zfsArgs = append(zfsArgs, "-o", "origin="+j.Origin)
	}

	zfsArgs = append(zfsArgs, j.LocalVolume)
	cmd := exec.CommandContext(ctx, "zfs", zfsArgs...)

	return cmd
}

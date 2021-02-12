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

package zfs

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
)

// ZFSPath is the path to the zfs binary
var (
	ZFSPath = "zfs"
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

// GetSnapshotsAndBookmarks will retrieve all snapshots and bookmarks for the given target
func GetSnapshotsAndBookmarks(ctx context.Context, target string) ([]files.SnapshotInfo, error) {
	errB := new(bytes.Buffer)
	cmd := exec.CommandContext(
		ctx, ZFSPath, "list", "-H", "-d", "1", "-p", "-t", "snapshot,bookmark", "-r", "-o", "name,creation,type", "-S", "creation", target,
	)
	log.AppLogger.Debugf("Getting ZFS Snapshots with command \"%s\"", strings.Join(cmd.Args, " "))
	cmd.Stderr = errB
	rpipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("%s (%v)", strings.TrimSpace(errB.String()), err)
	}
	var snapshots []files.SnapshotInfo
	for {
		snapInfo := files.SnapshotInfo{}
		var creation int64
		var objectType string
		n, nerr := fmt.Fscanln(rpipe, &snapInfo.Name, &creation, &objectType)
		if n == 0 || nerr != nil {
			break
		}
		snapInfo.CreationTime = time.Unix(creation, 0)
		if objectType == "bookmark" {
			snapInfo.Name = snapInfo.Name[strings.Index(snapInfo.Name, "#")+1:]
			snapInfo.Bookmark = true
		} else {
			snapInfo.Name = snapInfo.Name[strings.Index(snapInfo.Name, "@")+1:]
		}
		snapshots = append(snapshots, snapInfo)
	}
	err = cmd.Wait()
	if err != nil {
		return nil, fmt.Errorf("%s (%v)", strings.TrimSpace(errB.String()), err)
	}

	return snapshots, nil
}

// GetZFSProperty will return the raw value returned by the "zfs get" command for
// the given property on the given target.
func GetZFSProperty(ctx context.Context, prop, target string) (string, error) {
	b := new(bytes.Buffer)
	errB := new(bytes.Buffer)
	cmd := exec.CommandContext(ctx, ZFSPath, "get", "-H", "-p", "-o", "value", prop, target)
	log.AppLogger.Debugf("Getting ZFS Property with command \"%s\"", strings.Join(cmd.Args, " "))
	cmd.Stdout = b
	cmd.Stderr = errB
	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("%s (%v)", strings.TrimSpace(errB.String()), err)
	}
	return strings.TrimSpace(b.String()), nil
}

// GetZFSSendCommand will return the send command to use for the given JobInfo
func GetZFSSendCommand(ctx context.Context, j *files.JobInfo) *exec.Cmd {
	// Prepare the zfs send command
	zfsArgs := []string{"send"}

	if j.Replication {
		log.AppLogger.Infof("Enabling the replication (-R) flag on the send.")
		zfsArgs = append(zfsArgs, "-R")
	}

	if j.Deduplication {
		log.AppLogger.Infof("Enabling the deduplication (-D) flag on the send.")
		zfsArgs = append(zfsArgs, "-D")
	}

	if j.Properties {
		log.AppLogger.Infof("Enabling the properties (-p) flag on the send.")
		zfsArgs = append(zfsArgs, "-p")
	}

	if j.Compressor == files.ZfsCompressor {
		log.AppLogger.Infof("Enabling the compression (-c) flag on the send.")
		zfsArgs = append(zfsArgs, "-c")
	}

	if j.IncrementalSnapshot.Name != "" {
		incrementalName := j.IncrementalSnapshot.Name
		if j.IncrementalSnapshot.Bookmark {
			incrementalName = fmt.Sprintf("%s#%s", j.VolumeName, incrementalName)
		}

		if j.IntermediaryIncremental {
			log.AppLogger.Infof("Enabling an incremental stream with all intermediary snapshots (-I) on the send to snapshot %s", incrementalName)
			zfsArgs = append(zfsArgs, "-I", incrementalName)
		} else {
			log.AppLogger.Infof("Enabling an incremental stream (-i) on the send to snapshot %s", incrementalName)
			zfsArgs = append(zfsArgs, "-I", incrementalName)
		}
	}

	zfsArgs = append(zfsArgs, fmt.Sprintf("%s@%s", j.VolumeName, j.BaseSnapshot.Name))
	cmd := exec.CommandContext(ctx, ZFSPath, zfsArgs...)

	return cmd
}

// GetZFSReceiveCommand will return the recv command to use for the given JobInfo
func GetZFSReceiveCommand(ctx context.Context, j *files.JobInfo) *exec.Cmd {
	// Prepare the zfs send command
	zfsArgs := []string{"receive"}

	if j.FullPath {
		log.AppLogger.Infof("Enabling the full path (-d) flag on the receive.")
		zfsArgs = append(zfsArgs, "-d")
	}

	if j.LastPath {
		log.AppLogger.Infof("Enabling the last path (-e) flag on the receive.")
		zfsArgs = append(zfsArgs, "-e")
	}

	if j.NotMounted {
		log.AppLogger.Infof("Enabling the not mounted (-u) flag on the receive.")
		zfsArgs = append(zfsArgs, "-u")
	}

	if j.Force {
		log.AppLogger.Infof("Enabling the forced rollback (-F) flag on the receive.")
		zfsArgs = append(zfsArgs, "-F")
	}

	if j.Origin != "" {
		log.AppLogger.Infof("Enabling the origin flag (-o) on the receive to %s", j.Origin)
		zfsArgs = append(zfsArgs, "-o", "origin="+j.Origin)
	}

	zfsArgs = append(zfsArgs, j.LocalVolume)
	cmd := exec.CommandContext(ctx, ZFSPath, zfsArgs...)

	return cmd
}

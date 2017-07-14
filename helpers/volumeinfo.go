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
	"bufio"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/juju/ratelimit"
	gzip "github.com/klauspost/pgzip"
	"github.com/miolini/datacounter"
	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/packet"
)

var (
	printCompressCMD sync.Once
	// BackupUploadBucket is the bandwidth rate-limit bucket if we need one.
	BackupUploadBucket *ratelimit.Bucket
	// BackupTempdir is the scratch space for our output
	BackupTempdir string
	// WorkingDir is the directory that all the cache/scratch work is done for this program
	WorkingDir string
)

const (
	// BufferSize is the size of various buffers and copy limits around the applicaiton
	BufferSize = 256 * humanize.KiByte // 256KiB
)

// VolumeInfo holds all necessary information for a Volume as part of a backup
type VolumeInfo struct {
	ObjectName      string
	VolumeNumber    int64
	SHA256          hash.Hash   `json:"-"`
	MD5             hash.Hash   `json:"-"`
	CRC32C          hash.Hash32 `json:"-"`
	SHA256Sum       string
	MD5Sum          string
	CRC32CSum32     uint32
	Size            uint64
	ZFSStreamBytes  uint64
	CreateTime      time.Time
	CloseTime       time.Time
	IsManifest      bool
	IsFinalManifest bool

	filename string
	w        io.Writer
	r        io.Reader
	bufw     *bufio.Writer
	fw       *os.File
	// Pipe Objects
	pw *io.PipeWriter
	pr *io.PipeReader
	// (de)compressor objects
	cw  io.WriteCloser
	rw  io.ReadCloser
	cmd *exec.Cmd
	// PGP objects
	pgpw io.WriteCloser
	pgpr *openpgp.MessageDetails
	// Detail Objects
	counter   *datacounter.WriterCounter
	usingPipe bool
	isClosed  bool
	isOpened  bool
	lock      sync.Mutex
}

// ByVolumeNumber is used to sort a VolumeInfo slice by VolumeNumber.
type ByVolumeNumber []*VolumeInfo

func (a ByVolumeNumber) Len() int           { return len(a) }
func (a ByVolumeNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByVolumeNumber) Less(i, j int) bool { return a[i].VolumeNumber < a[j].VolumeNumber }

// Counter will return how many bytes have been written to this volume.
func (v *VolumeInfo) Counter() uint64 {
	return v.counter.Count()
}

// Read will passthru the command to the underlying io.Reader, which will be setup
// to ratelimit where applicable.
func (v *VolumeInfo) Read(p []byte) (int, error) {
	i, err := v.r.Read(p)
	if err == io.EOF && v.pgpr != nil {
		if v.pgpr.IsSigned {
			if v.pgpr.SignatureError != nil {
				return i, v.pgpr.SignatureError
			}
			if v.pgpr.SignedBy == nil {
				return i, fmt.Errorf("did not have ths key signature to verify the message with")
			}
		}
	}
	return i, err
}

// Seek will passthru the command to the underlying *os.File
func (v *VolumeInfo) Seek(offset int64, whence int) (int64, error) {
	return v.fw.Seek(offset, whence)
}

// OpenVolume will open this VolumeInfo in a read-only mode. It will automatically
// rate limit the amount of bytes that can be read at a time so no buffer should
// be used for reading from this Reader.
// Only valid to be called after creating a new Volume and closing it or when
// a MaxFileBuffer of 0 in which case this does nothing.
func (v *VolumeInfo) OpenVolume() error {
	if v.isOpened {
		return nil
	}
	f, err := os.Open(v.filename)
	if err != nil {
		return err
	}
	v.fw = f
	v.r = f
	v.isClosed = false
	v.isOpened = true
	if BackupUploadBucket != nil {
		v.r = ratelimit.Reader(v.r, BackupUploadBucket)
	}

	return nil
}

// ExtractLocal will try and open a local file for extraction
func ExtractLocal(ctx context.Context, j *JobInfo, path string) (*VolumeInfo, error) {
	v := new(VolumeInfo)
	v.filename = path
	err := v.Extract(ctx, j)
	return v, err
}

// Extract will setup the volume for reading such that reading from it will handle any
// decryption, signature verification, and decompression that was used on it.
func (v *VolumeInfo) Extract(ctx context.Context, j *JobInfo) error {
	f, err := os.Open(v.filename)
	if err != nil {
		return err
	}
	v.fw = f
	v.r = f
	v.isClosed = false
	v.isOpened = true

	if j.EncryptKey != nil || j.SignKey != nil {
		config := new(packet.Config)
		config.DefaultCompressionAlgo = packet.CompressionNone // We will do our own, thank you very much!
		config.DefaultCipher = packet.CipherAES256
		pgpReader, perr := openpgp.ReadMessage(v.r, getCombinedKeyRing(), promptFunc, config)
		if perr != nil {
			return perr
		}
		v.pgpr = pgpReader
		v.r = pgpReader.UnverifiedBody
	}

	switch j.Compressor {
	case "internal":
		v.rw, err = gzip.NewReader(v.r)
		if err != nil {
			return err
		}
		v.r = v.rw
	case "":
	default:
		v.cmd = exec.CommandContext(ctx, j.Compressor, "-c", "-d")
		v.cmd.Stdin = v.r

		decompressor, err := v.cmd.StdoutPipe()
		if err != nil {
			return err
		}
		v.rw = decompressor
		v.r = v.rw
		v.cmd.Stderr = os.Stderr

		v.cmd.Start()
	}
	return nil
}

// DeleteVolume will delete the volume from the temporary directory it was written to.
// Only valid to be called after creating a new Volume and closing it.
func (v *VolumeInfo) DeleteVolume() error {
	return os.Remove(v.filename)
}

// Write writes through to the underlying writer, satisfying the io.Writer interface.
func (v *VolumeInfo) Write(p []byte) (int, error) {
	return v.w.Write(p)
}

// Close should be called after creating a new volume or after calling OpenVolume
func (v *VolumeInfo) Close() error {
	// Protect against multiple calls to this function
	v.lock.Lock()
	defer v.lock.Unlock()

	if v.isClosed {
		return nil
	}
	v.isClosed = true

	if !v.isOpened || v.pw != nil {
		v.CloseTime = time.Now()
	}

	if v.isOpened {
		v.isOpened = false
	}

	// Close the (de)compressor, if any
	if v.cw != nil || v.rw != nil {
		if v.cw != nil {
			if err := v.cw.Close(); err != nil {
				return err
			}
			v.cw = nil
		}

		if v.rw != nil {
			if err := v.rw.Close(); err != nil {
				return err
			}
			v.rw = nil
		}

		// If we used an external (de)compressor, wait for it to close as well
		if v.cmd != nil {
			if err := v.cmd.Wait(); err != nil {
				return err
			}
			v.cmd = nil
		}
	}

	// Close the (de/en)crypter, if any
	if v.pgpw != nil || v.pgpr != nil {
		if v.pgpw != nil {
			if err := v.pgpw.Close(); err != nil {
				return err
			}
			v.pgpw = nil
		}

		if v.pgpr != nil {
			v.pgpw = nil
		}
	}

	// Flush the buffered writer
	if v.bufw != nil {
		v.bufw.Flush()
		v.bufw = nil
	}

	// Finally, close the actual file or Pipe
	if v.fw != nil {
		if err := v.fw.Close(); err != nil {
			return err
		}
		v.fw = nil
	}

	if v.pw != nil {
		// Special case for when we are using pipes, make the volume think its still
		// open and needs to be closed by the reader.
		v.isClosed = false
		v.isOpened = true
		if err := v.pw.Close(); err != nil {
			return err
		}
		v.pw = nil
	} else if v.pr != nil {
		if err := v.pr.Close(); err != nil {
			return err
		}
		v.pr = nil
	}

	// Record computed metrics and release resources
	if v.counter != nil {
		v.Size = v.counter.Count()
		v.counter = nil
	}

	if v.SHA256 != nil {
		v.SHA256Sum = fmt.Sprintf("%x", v.SHA256.Sum(nil))
		v.SHA256 = nil
	}

	if v.CRC32C != nil {
		v.CRC32CSum32 = v.CRC32C.Sum32()
		v.CRC32C = nil
	}

	if v.MD5 != nil {
		v.MD5Sum = fmt.Sprintf("%x", v.MD5.Sum(nil))
		v.MD5 = nil
	}

	v.w = nil
	if v.pr == nil {
		v.r = nil
	}

	return nil
}

// CopyTo will write out the volume to the path specified
func (v *VolumeInfo) CopyTo(dest string) (err error) {
	in, err := os.Open(v.filename)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dest)
	if err != nil {
		return
	}
	defer out.Close()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

// prepareVolume returns a VolumeInfo, filename parts, extension parts, and an error
// compress -> encrypt/sign -> output
func prepareVolume(ctx context.Context, j *JobInfo, pipe bool) (*VolumeInfo, []string, []string, error) {
	v, err := CreateSimpleVolume(ctx, pipe)
	if err != nil {
		return nil, nil, nil, err
	}

	extensions := make([]string, 0, 2)

	// Prepare the Encryption/Signing writer, if required
	if j.EncryptKey != nil || j.SignKey != nil {
		extensions = append(extensions, "pgp")
		config := new(packet.Config)
		config.DefaultCompressionAlgo = packet.CompressionNone // We will do our own, thank you very much!
		config.DefaultCipher = packet.CipherAES256
		fileHints := new(openpgp.FileHints)
		fileHints.IsBinary = true
		pgpWriter, err := openpgp.Encrypt(v.w, []*openpgp.Entity{j.EncryptKey}, j.SignKey, fileHints, config)
		if err != nil {
			return nil, nil, nil, err
		}
		v.pgpw = pgpWriter
		v.w = pgpWriter
	}

	// Prepare the compression writer, if any
	switch j.Compressor {
	case "internal":
		v.cw, _ = gzip.NewWriterLevel(v.w, j.CompressionLevel)
		v.w = v.cw
		extensions = append([]string{"gz"}, extensions...)
		printCompressCMD.Do(func() {
			AppLogger.Infof("Will be using internal gzip compressor with compression level %d.", j.CompressionLevel)
		})
	case "":
		printCompressCMD.Do(func() { AppLogger.Infof("Will not be using any compression.") })
	default:
		extensions = append([]string{j.Compressor}, extensions...)

		v.cmd = exec.CommandContext(ctx, j.Compressor, "-c", fmt.Sprintf("-%d", j.CompressionLevel))
		v.cmd.Stdout = v.w

		compressor, err := v.cmd.StdinPipe()
		if err != nil {
			return nil, nil, nil, err
		}
		v.cw = compressor
		v.w = v.cw
		v.cmd.Stderr = os.Stderr

		printCompressCMD.Do(func() {
			AppLogger.Infof("Will be using the external binary %s for compression with compression level %d. The executing command will be: %s", j.Compressor, j.CompressionLevel, strings.Join(v.cmd.Args, " "))
		})

		v.cmd.Start()
	}

	nameParts := []string{j.VolumeName}
	if j.IncrementalSnapshot.Name != "" {
		nameParts = append(nameParts, j.IncrementalSnapshot.Name, "to", j.BaseSnapshot.Name)
	} else {
		nameParts = append(nameParts, j.BaseSnapshot.Name)
	}

	return v, nameParts, extensions, nil
}

// CreateManifestVolume will call CreateSimpleVolume and add options to compress,
// encrypt, and/or sign the file as it is written depending on the provided options.
// It will also name the file accordingly as a manifest file.
func CreateManifestVolume(ctx context.Context, j *JobInfo) (*VolumeInfo, error) {
	// Create and name the manifest file
	extensions := []string{"manifest"}
	nameParts := []string{j.ManifestPrefix}

	v, baseParts, ext, err := prepareVolume(ctx, j, false)
	if err != nil {
		return nil, err
	}

	extensions = append(extensions, ext...)
	nameParts = append(nameParts, baseParts...)

	v.ObjectName = fmt.Sprintf("%s.%s", strings.Join(nameParts, j.Separator), strings.Join(extensions, "."))
	v.IsManifest = true

	return v, nil
}

// CreateBackupVolume will call CreateSimpleVolume and add options to compress,
// encrypt, and/or sign the file as it is written depending on the provided options.
// It will also name the file accordingly as a volume as part of backup set.
func CreateBackupVolume(ctx context.Context, j *JobInfo, volnum int64) (*VolumeInfo, error) {
	// Create and name the backup file
	extensions := []string{"zstream"}

	pipe := false
	if j.MaxFileBuffer == 0 {
		pipe = true
	}

	v, nameParts, ext, err := prepareVolume(ctx, j, pipe)
	if err != nil {
		return nil, err
	}

	v.VolumeNumber = volnum
	extensions = append(extensions, ext...)
	extensions = append(extensions, fmt.Sprintf("vol%d", v.VolumeNumber))

	v.ObjectName = fmt.Sprintf("%s.%s", strings.Join(nameParts, j.Separator), strings.Join(extensions, "."))

	return v, nil
}

// CreateSimpleVolume will create a temporary file to write to. If
// MaxParallelUploads is set to 0, no temporary file will be used and an OS Pipe
// will be used instead.
func CreateSimpleVolume(ctx context.Context, pipe bool) (*VolumeInfo, error) {
	v := &VolumeInfo{
		SHA256:     sha256.New(),
		CRC32C:     crc32.New(crc32.MakeTable(crc32.Castagnoli)),
		MD5:        md5.New(),
		CreateTime: time.Now(),
	}

	if pipe {
		v.pr, v.pw = io.Pipe()
		v.r = v.pr
		v.w = v.pw
		v.isOpened = true
		v.usingPipe = true
		if BackupUploadBucket != nil {
			v.r = ratelimit.Reader(v.r, BackupUploadBucket)
		}
	} else {
		tempFile, err := ioutil.TempFile(BackupTempdir, LogModuleName)
		if err != nil {
			return nil, err
		}
		v.fw = tempFile
		v.filename = tempFile.Name()
		v.w = v.fw
	}

	// Buffer the writes to double the default block size (128KB)
	v.bufw = bufio.NewWriterSize(v.w, BufferSize)
	v.w = v.bufw

	// Compute hashes
	v.w = io.MultiWriter(v.w, v.SHA256, v.CRC32C, v.MD5)

	// Add a writer that counts how many bytes have been written
	v.counter = datacounter.NewWriterCounter(v.w)
	v.w = v.counter

	return v, nil
}

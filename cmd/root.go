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
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	humanize "github.com/dustin/go-humanize"
	"github.com/juju/ratelimit"
	"github.com/op/go-logging"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/someone1/zfsbackup-go/helpers"
)

var (
	numCores          int
	logLevel          string
	secretKeyRingPath string
	publicKeyRingPath string
	workingDirectory  string
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "zfsbackup",
	Short: "zfsbackup is a tool used to do off-site backups of ZFS volumes.",
	Long: `zfsbackup is a tool used to do off-site backups of ZFS volumes.
It leverages the built-in snapshot capabilities of ZFS in order to export ZFS
volumes for long-term storage.

zfsbackup uses the "zfs send" command to export, and optionally compress, sign,
encrypt, and split the send stream to files that are then transferred to a
destination of your choosing.`,
	PersistentPreRun: processFlags,
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	RootCmd.PersistentFlags().IntVar(&numCores, "numCores", 2, "number of CPU cores to utilize. Do not exceed the number of CPU cores on the system.")
	RootCmd.PersistentFlags().StringVar(&logLevel, "logLevel", "notice", "this controls the verbosity level of logging. Possible values are critical, error, warning, notice, info, debug.")
	RootCmd.PersistentFlags().StringVar(&secretKeyRingPath, "secretKeyRingPath", "", "the path to the PGP secret key ring")
	RootCmd.PersistentFlags().StringVar(&publicKeyRingPath, "publicKeyRingPath", "", "the path to the PGP public key ring")
	RootCmd.PersistentFlags().StringVar(&workingDirectory, "workingDirectory", "~/.zfsbackup", "the working directory path for zfsbackup.")
	RootCmd.PersistentFlags().StringVar(&jobInfo.ManifestPrefix, "manifestPrefix", "manifests", "the prefix to use for all manifest files.")
	RootCmd.PersistentFlags().StringVar(&jobInfo.Compressor, "compressor", "internal", "specify to use the internal (parallel) gzip implementation or an external binary (e.g. gzip, bzip2, pigz, lzma, xz, etc. Syntax must be similiar to the gzip compression tool) to compress the stream for storage. Please take into consideration time, memory, and CPU usage for any of the compressors used.")
	RootCmd.PersistentFlags().StringVar(&jobInfo.EncryptTo, "encryptTo", "", "the email of the user to encrypt the data to from the provided public keyring.")
	RootCmd.PersistentFlags().StringVar(&jobInfo.SignFrom, "signFrom", "", "the email of the user to sign on behalf of from the provided private keyring.")
	passphrase = []byte(os.Getenv("PGP_PASSPHRASE"))
}

func processFlags(cmd *cobra.Command, args []string) {
	switch strings.ToLower(logLevel) {
	case "critical":
		logging.SetLevel(logging.CRITICAL, helpers.LogModuleName)
	case "error":
		logging.SetLevel(logging.ERROR, helpers.LogModuleName)
	case "warning", "warn":
		logging.SetLevel(logging.WARNING, helpers.LogModuleName)
	case "notice":
		logging.SetLevel(logging.NOTICE, helpers.LogModuleName)
	case "info":
		logging.SetLevel(logging.INFO, helpers.LogModuleName)
	case "debug":
		logging.SetLevel(logging.DEBUG, helpers.LogModuleName)
	default:
		helpers.AppLogger.Errorf("Invalid log level provided. Was given %s", logLevel)
		panic(helpers.Exit{Code: 10})
	}

	if numCores <= 0 {
		helpers.AppLogger.Errorf("The number of cores to use provided is an invalid value. It must be greater than 0. %d was given.", numCores)
		panic(helpers.Exit{Code: 10})
	}

	if numCores > runtime.NumCPU() {
		helpers.AppLogger.Warningf("Ignoring user provided number of cores (%d) and using the number of detected cores (%d).", numCores, runtime.NumCPU())
		numCores = runtime.NumCPU()
	}
	helpers.AppLogger.Infof("Setting number of cores to: %d", numCores)
	runtime.GOMAXPROCS(numCores)

	if secretKeyRingPath != "" {
		if err := helpers.LoadPrivateRing(secretKeyRingPath); err != nil {
			helpers.AppLogger.Errorf("Could not load private keyring due to an error - %v", err)
			panic(helpers.Exit{Code: 10})
		}
	}
	helpers.AppLogger.Infof("Loaded private key ring %s", secretKeyRingPath)

	if publicKeyRingPath != "" {
		if err := helpers.LoadPublicRing(publicKeyRingPath); err != nil {
			helpers.AppLogger.Errorf("Could not load public keyring due to an error - %v", err)
			panic(helpers.Exit{Code: 10})
		}
	}
	helpers.AppLogger.Infof("Loaded public key ring %s", publicKeyRingPath)

	if jobInfo.EncryptTo != "" && secretKeyRingPath == "" {
		helpers.AppLogger.Errorf("You must specify a private keyring path if you provide an encryptFrom option")
		panic(helpers.Exit{Code: 10})
	}

	if jobInfo.SignFrom != "" && publicKeyRingPath == "" {
		helpers.AppLogger.Errorf("You must specify a public keyring path if you provide an signTo option")
		panic(helpers.Exit{Code: 10})
	}

	if jobInfo.EncryptTo != "" {
		if jobInfo.EncryptKey = helpers.GetPrivateKeyByEmail(jobInfo.EncryptTo); jobInfo.EncryptKey == nil {
			helpers.AppLogger.Errorf("Could not find public key for %s", jobInfo.EncryptTo)
			panic(helpers.Exit{Code: 10})
		}

		if jobInfo.EncryptKey.PrivateKey != nil && jobInfo.EncryptKey.PrivateKey.Encrypted {
			validatePassphrase()
			if err := jobInfo.EncryptKey.PrivateKey.Decrypt(passphrase); err != nil {
				helpers.AppLogger.Errorf("Error decrypting private key: %v", err)
				panic(helpers.Exit{Code: 10})
			}
		}

		for _, subkey := range jobInfo.EncryptKey.Subkeys {
			if subkey.PrivateKey != nil && subkey.PrivateKey.Encrypted {
				validatePassphrase()
				if err := subkey.PrivateKey.Decrypt(passphrase); err != nil {
					helpers.AppLogger.Errorf("Error decrypting subkey's private key: %v", err)
					panic(helpers.Exit{Code: 10})
				}
			}
		}
	}

	if jobInfo.SignFrom != "" {
		if jobInfo.SignKey = helpers.GetPrivateKeyByEmail(jobInfo.SignFrom); jobInfo.SignKey == nil {
			helpers.AppLogger.Errorf("Could not find private key for %s", jobInfo.SignFrom)
			panic(helpers.Exit{Code: 10})
		}

		if jobInfo.SignKey.PrivateKey != nil && jobInfo.SignKey.PrivateKey.Encrypted {
			validatePassphrase()
			if err := jobInfo.SignKey.PrivateKey.Decrypt(passphrase); err != nil {
				helpers.AppLogger.Errorf("Error decrypting private key: %v", err)
				panic(helpers.Exit{Code: 10})
			}
		}

		for _, subkey := range jobInfo.SignKey.Subkeys {
			if subkey.PrivateKey != nil && subkey.PrivateKey.Encrypted {
				validatePassphrase()
				if err := subkey.PrivateKey.Decrypt(passphrase); err != nil {
					helpers.AppLogger.Errorf("Error decrypting subkey's private key: %v", err)
					panic(helpers.Exit{Code: 10})
				}
			}
		}
	}

	setupGlobalVars()
	helpers.AppLogger.Infof("Setting working directory to %s", workingDirectory)
	helpers.PrintPGPDebugInformation()
}

func setupGlobalVars() {
	// Setup Tempdir

	if strings.HasPrefix(workingDirectory, "~") {
		usr, err := user.Current()
		if err != nil {
			helpers.AppLogger.Errorf("Could not get current user due to error - %v", err)
			panic(helpers.Exit{Code: 21})
		}
		workingDirectory = filepath.Join(usr.HomeDir, strings.TrimPrefix(workingDirectory, "~"))
	}

	if dir, serr := os.Stat(workingDirectory); serr == nil && !dir.IsDir() {
		helpers.AppLogger.Errorf("Cannot create working directory because another non-directory object already exists in that path (%s)", workingDirectory)
		panic(helpers.Exit{Code: 19})
	} else if serr != nil {
		err := os.Mkdir(workingDirectory, 0755)
		if err != nil {
			helpers.AppLogger.Errorf("Could not create working directory %s due to error - %v", workingDirectory, err)
			panic(helpers.Exit{Code: 20})
		}
	}

	dirPath := filepath.Join(workingDirectory, "temp")
	if dir, serr := os.Stat(dirPath); serr == nil && !dir.IsDir() {
		helpers.AppLogger.Errorf("Cannot create temp dir in working directory because another non-directory object already exists in that path (%s)", dirPath)
		panic(helpers.Exit{Code: 18})
	} else if serr != nil {
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			helpers.AppLogger.Errorf("Could not create temp directory %s due to error - %v", dirPath, err)
			panic(helpers.Exit{Code: 3})
		}
	}

	tempdir, err := ioutil.TempDir(dirPath, helpers.LogModuleName)
	if err != nil {
		helpers.AppLogger.Errorf("Could not create temp directory due to error - %v", err)
		panic(helpers.Exit{Code: 4})
	}

	helpers.BackupTempdir = tempdir
	helpers.WorkingDir = workingDirectory

	dirPath = filepath.Join(workingDirectory, "cache")
	if dir, serr := os.Stat(dirPath); serr == nil && !dir.IsDir() {
		helpers.AppLogger.Errorf("Cannot create cache dir in working directory because another non-directory object already exists in that path (%s)", dirPath)
		panic(helpers.Exit{Code: 18})
	} else if serr != nil {
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			helpers.AppLogger.Errorf("Could not create cache directory %s due to error - %v", dirPath, err)
			panic(helpers.Exit{Code: 3})
		}
	}

	if maxUploadSpeed != 0 {
		helpers.AppLogger.Infof("Limiting the upload speed to %s/s.", humanize.Bytes(maxUploadSpeed*humanize.KByte))
		helpers.BackupUploadBucket = ratelimit.NewBucketWithRate(float64(maxUploadSpeed*humanize.KByte), int64(maxUploadSpeed*humanize.KByte))
	}

}

func validatePassphrase() {
	var err error
	if len(passphrase) == 0 {
		fmt.Print("Enter passphrase to decrypt encryption key: ")
		passphrase, err = terminal.ReadPassword(0)
		if err != nil {
			helpers.AppLogger.Errorf("Error reading user input for encryption key passphrase: %v", err)
			panic(helpers.Exit{Code: 10})
		}
	}
}

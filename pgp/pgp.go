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

package pgp

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/openpgp"

	"github.com/someone1/zfsbackup-go/log"
)

var (
	pubRing openpgp.EntityList
	secRing openpgp.EntityList
)

// GetPublicKeyByEmail will return the key from the pubpoic PGP ring (if available) matching
// the provided email address.
func GetPublicKeyByEmail(email string) *openpgp.Entity {
	return getKeyByEmail(pubRing, email)
}

// GetPrivateKeyByEmail will return the key from the secret PGP ring (if available) matching
// the provided email address.
func GetPrivateKeyByEmail(email string) *openpgp.Entity {
	return getKeyByEmail(secRing, email)
}

// GetCombinedKeyRing will return both the public and secret key rings combined
func GetCombinedKeyRing() openpgp.KeyRing {
	return append(pubRing, secRing...)
}

// PromptFunc is used to satisfy the openpgp package's requirements
func PromptFunc(keys []openpgp.Key, symmetric bool) ([]byte, error) {
	panic("secret keys should have been decrypted already")
}

func getKeyByEmail(keyring openpgp.EntityList, email string) *openpgp.Entity {
	for _, entity := range keyring {
		for _, ident := range entity.Identities {
			if ident.UserId.Email == email {
				return entity
			}
		}
	}

	return nil
}

// LoadPublicRing will open and parse the PGP keyring from the file path provided.
func LoadPublicRing(path string) error {
	pubringFile, err := os.Open(path)
	if err != nil {
		return err
	}
	pubRing, err = openpgp.ReadArmoredKeyRing(pubringFile)
	return err
}

// LoadPrivateRing will open and parse the PGP keyring from the file path provided.
func LoadPrivateRing(path string) error {
	privringFile, err := os.Open(path)
	if err != nil {
		return err
	}
	secRing, err = openpgp.ReadArmoredKeyRing(privringFile)

	return err
}

// PrintPGPDebugInformation will output a debug log entry listing the keys it has read in from each keyring.
func PrintPGPDebugInformation() {
	debugStr := make([]string, 0, 4)
	debugStr = append(debugStr, "PGP Debug Info:\nLoaded Private Keys:")
	for _, key := range secRing {
		debugStr = append(debugStr, fmt.Sprintf("\t%v\n\t%v", key.PrimaryKey.KeyIdString(), key.Identities))
	}

	debugStr = append(debugStr, "\nLoaded Public Keys:")
	for _, key := range pubRing {
		debugStr = append(debugStr, fmt.Sprintf("\t%v\n\t%v", key.PrimaryKey.KeyIdString(), key.Identities))
	}

	log.AppLogger.Debugf("%s", strings.Join(debugStr, "\n"))
}

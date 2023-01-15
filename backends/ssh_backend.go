package backends

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"github.com/someone1/zfsbackup-go/files"
	"github.com/someone1/zfsbackup-go/log"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

const SSHBackendPrefix = "ssh"

type SSHBackend struct {
	conf       *BackendConfig
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	remotePath string
}

func buildSshSigner(privateKeyPath string, password string) (ssh.Signer, error) {
	privateKey, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, err
	}

	signer, err := ssh.ParsePrivateKey(privateKey)
	_, isMissingPassword := err.(*ssh.PassphraseMissingError)
	if isMissingPassword && password != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(privateKey, []byte(password))
	}

	return signer, err
}

func buildAuthMethods(userHomeDir string, password string) (sshAuths []ssh.AuthMethod, err error) {
	sshAuthSock := os.Getenv("SSH_AUTH_SOCK")
	if sshAuthSock != "" {
		sshAgent, err := net.Dial("unix", sshAuthSock)
		if err != nil {
			return nil, err
		}
		sshAuths = append(sshAuths, ssh.PublicKeysCallback(agent.NewClient(sshAgent).Signers))
	}

	sshKeyFile := os.Getenv("SSH_KEY_FILE")
	if sshKeyFile != "" {
		signer, err := buildSshSigner(sshKeyFile, password)
		if err != nil {
			return nil, err
		}
		sshAuths = append(sshAuths, ssh.PublicKeys(signer))
	} else {
		signers := make([]ssh.Signer, 0)

		defaultKeys := []string{
			filepath.Join(userHomeDir, ".ssh/id_rsa"),
			filepath.Join(userHomeDir, ".ssh/id_cdsa"),
			filepath.Join(userHomeDir, ".ssh/id_ecdsa_sk"),
			filepath.Join(userHomeDir, ".ssh/id_ed25519"),
			filepath.Join(userHomeDir, ".ssh/id_ed25519_sk"),
			filepath.Join(userHomeDir, ".ssh/id_dsa"),
		}

		for _, keyPath := range defaultKeys {
			signer, err := buildSshSigner(keyPath, password)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					log.AppLogger.Warningf("ssh backend: Failed to use ssh key at %s - %v", keyPath, err)
				}
				continue
			}
			signers = append(signers, signer)
		}
		if len(signers) > 0 {
			sshAuths = append(sshAuths, ssh.PublicKeys(signers...))
		}
	}

	if password != "" {
		sshAuths = append(sshAuths, ssh.Password(password))
	}

	return sshAuths, nil
}

func buildHostKeyCallback(userHomeDir string) (callback ssh.HostKeyCallback, err error) {
	knownHostsFile := os.Getenv("SSH_KNOWN_HOSTS")
	if knownHostsFile == "" {
		knownHostsFile = filepath.Join(userHomeDir, ".ssh/known_hosts")
	}
	if knownHostsFile == "ignore" {
		callback = ssh.InsecureIgnoreHostKey()
	} else {
		callback, err = knownhosts.New(knownHostsFile)
	}
	return callback, err
}

func (s *SSHBackend) Init(ctx context.Context, conf *BackendConfig, opts ...Option) (err error) {
	s.conf = conf

	if !strings.HasPrefix(s.conf.TargetURI, SSHBackendPrefix+"://") {
		return ErrInvalidURI
	}

	targetUrl, err := url.Parse(s.conf.TargetURI)
	if err != nil {
		log.AppLogger.Errorf("ssh backend: Error while parsing target uri %s - %v", s.conf.TargetURI, err)
		return err
	}

	s.remotePath = strings.TrimSuffix(targetUrl.Path, "/")
	if s.remotePath == "" && targetUrl.Path != "/" { // allow root path
		log.AppLogger.Errorf("ssh backend: No remote path provided!")
		return ErrInvalidURI
	}

	username := os.Getenv("SSH_USERNAME")
	password := os.Getenv("SSH_PASSWORD")
	if targetUrl.User != nil {
		urlUsername := targetUrl.User.Username()
		if urlUsername != "" {
			username = urlUsername
		}
		urlPassword, _ := targetUrl.User.Password()
		if urlPassword != "" {
			password = urlPassword
		}
	}

	userInfo, err := user.Current()
	if err != nil {
		return err
	}
	if username == "" {
		username = userInfo.Username
	}

	sshAuths, err := buildAuthMethods(userInfo.HomeDir, password)
	if err != nil {
		return err
	}

	hostKeyCallback, err := buildHostKeyCallback(userInfo.HomeDir)
	if err != nil {
		return err
	}

	sshConfig := &ssh.ClientConfig{
		User:            username,
		Auth:            sshAuths,
		HostKeyCallback: hostKeyCallback,
		Timeout:         30 * time.Second,
	}

	hostname := targetUrl.Host
	if !strings.Contains(hostname, ":") {
		hostname = hostname + ":22"
	}
	s.sshClient, err = ssh.Dial("tcp", hostname, sshConfig)
	if err != nil {
		return err
	}

	s.sftpClient, err = sftp.NewClient(s.sshClient)
	if err != nil {
		return err
	}

	fi, err := s.sftpClient.Stat(s.remotePath)
	if err != nil {
		log.AppLogger.Errorf("ssh backend: Error while verifying remote path %s - %v", s.remotePath, err)
		return err
	}

	if !fi.IsDir() {
		log.AppLogger.Errorf("ssh backend: Provided remote path is not a directory!")
		return ErrInvalidURI
	}

	return nil
}

func (s *SSHBackend) Upload(ctx context.Context, vol *files.VolumeInfo) error {
	s.conf.MaxParallelUploadBuffer <- true
	defer func() {
		<-s.conf.MaxParallelUploadBuffer
	}()

	destinationPath := filepath.Join(s.remotePath, vol.ObjectName)
	destinationDir := filepath.Dir(destinationPath)

	if err := s.sftpClient.MkdirAll(destinationDir); err != nil {
		log.AppLogger.Debugf("ssh backend: Could not create path %s due to error - %v", destinationDir, err)
		return err
	}

	w, err := s.sftpClient.Create(destinationPath)
	if err != nil {
		log.AppLogger.Debugf("ssh backend: Could not create file %s due to error - %v", destinationPath, err)
		return err
	}

	_, err = io.Copy(w, vol)
	if err != nil {
		if closeErr := w.Close(); closeErr != nil {
			log.AppLogger.Warningf("ssh backend: Error closing volume %s - %v", vol.ObjectName, closeErr)
		}
		if deleteErr := os.Remove(destinationPath); deleteErr != nil {
			log.AppLogger.Warningf("ssh backend: Error deleting failed upload file %s - %v", destinationPath, deleteErr)
		}
		log.AppLogger.Debugf("ssh backend: Error while copying volume %s - %v", vol.ObjectName, err)
		return err
	}

	return w.Close()
}

func (s *SSHBackend) List(ctx context.Context, prefix string) ([]string, error) {
	l := make([]string, 0, 1000)

	w := s.sftpClient.Walk(s.remotePath)
	for w.Step() {
		if err := w.Err(); err != nil {
			return l, err
		}

		trimmedPath := strings.TrimPrefix(w.Path(), s.remotePath+string(filepath.Separator))
		if !w.Stat().IsDir() && strings.HasPrefix(trimmedPath, prefix) {
			l = append(l, trimmedPath)
		}
	}

	return l, nil
}

func (s *SSHBackend) Close() (err error) {
	if s.sftpClient != nil {
		err = s.sftpClient.Close()
		s.sftpClient = nil
	}
	if s.sshClient != nil {
		sshErr := s.sshClient.Close()
		if sshErr == nil && err == nil {
			err = sshErr
		}
		s.sshClient = nil
	}
	return err
}

func (s *SSHBackend) PreDownload(ctx context.Context, objects []string) error {
	return nil
}

func (s *SSHBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	return s.sftpClient.Open(filepath.Join(s.remotePath, filename))
}

func (s *SSHBackend) Delete(ctx context.Context, filename string) error {
	return s.sftpClient.Remove(filepath.Join(s.remotePath, filename))
}

var _ Backend = (*SSHBackend)(nil)

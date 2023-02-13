package backends

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ed25519"
	"golang.org/x/crypto/ssh"
)

func generateSSHPrivateKey() (ssh.Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}

	signer, err := ssh.ParsePrivateKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}))
	if err != nil {
		return nil, err
	}

	return signer, nil
}

// startSftpServer starts a very hacky sftp server based on
// https://github.com/pkg/sftp/blob/v1.13.5/examples/go-sftp-server/main.go
func startSftpServer(t testing.TB, user string, password string) net.Listener {
	t.Helper()

	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() == user && string(pass) == password {
				return nil, nil
			}
			return nil, fmt.Errorf("password rejected for %q", c.User())
		},
	}
	signer, err := generateSSHPrivateKey()
	if err != nil {
		t.Fatal("failed to generate key", err)
	}
	config.AddHostKey(signer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("failed to listen for connection", err)
	}

	fmt.Println("Listening on", listener.Addr())

	go func() {
		for {
			acceptedConn, err := listener.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					t.Fatal("failed to accept incoming connection", err)
				}
				return
			}

			go func(conn net.Conn) {
				// Before use, a handshake must be performed on the incoming net.Conn.
				_, chans, reqs, err := ssh.NewServerConn(conn, config)
				if err != nil {
					t.Fatal("failed to handshake", err)
				}
				fmt.Println("SSH server established")

				// The incoming Request channel must be serviced.
				go ssh.DiscardRequests(reqs)

				// Service the incoming Channel channel.
				for newChannel := range chans {
					// Channels have a type, depending on the application level
					// protocol intended. In the case of an SFTP session, this is "subsystem"
					// with a payload string of "<length=4>sftp"
					fmt.Println("Incoming channel:", newChannel.ChannelType())
					if newChannel.ChannelType() != "session" {
						_ = newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
						fmt.Println("Unknown channel type:", newChannel.ChannelType())
						continue
					}
					channel, requests, err := newChannel.Accept()
					if err != nil {
						t.Fatal("could not accept channel.", err)
					}
					fmt.Println("Channel accepted")

					// Sessions have out-of-band requests such as "shell",
					// "pty-req" and "env".  Here we handle only the
					// "subsystem" request.
					go func(in <-chan *ssh.Request) {
						for req := range in {
							fmt.Println("Request:", req.Type)
							ok := false
							switch req.Type {
							case "subsystem":
								fmt.Printf("Subsystem: %s\n", req.Payload[4:])
								if string(req.Payload[4:]) == "sftp" {
									ok = true
								}
							}
							fmt.Println(" - accepted:", ok)
							_ = req.Reply(ok, nil)
						}
					}(requests)

					server, err := sftp.NewServer(channel)
					if err != nil {
						t.Fatal(err)
					}
					if err := server.Serve(); err == io.EOF {
						_ = server.Close()
						fmt.Println("sftp client exited session.")
					} else if err != nil {
						t.Fatal("sftp server completed with error:", err)
					}
				}
			}(acceptedConn)
		}
	}()

	return listener
}

func TestSSHBackend(t *testing.T) {
	t.Parallel()

	sftpListener := startSftpServer(t, "test", "password")
	defer func() {
		_ = sftpListener.Close()
	}()

	tempPath := t.TempDir()

	backendUriEnd := "test:password@" + sftpListener.Addr().String() + tempPath

	err := os.Setenv("SSH_KNOWN_HOSTS", "ignore")
	if err != nil {
		t.Fatalf("Failed to disable host key checking: %v", err)
	}

	b, err := GetBackendForURI(SSHBackendPrefix + "://" + backendUriEnd)
	if err != nil {
		t.Fatalf("Error while trying to get backend: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	BackendTest(ctx, SSHBackendPrefix, backendUriEnd, true, b)(t)
}

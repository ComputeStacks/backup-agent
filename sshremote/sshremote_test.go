package sshremote

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

func generateKey() ([]byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	return keyPEM, nil
}

func startSSHServer(t *testing.T) (string, string, func()) {
	// Generate host key
	hostKeyPEM, err := generateKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}
	hostKey, err := ssh.ParsePrivateKey(hostKeyPEM)
	if err != nil {
		t.Fatalf("Failed to parse host key: %v", err)
	}

	// Generate user key
	userKeyPEM, err := generateKey()
	if err != nil {
		t.Fatalf("Failed to generate user key: %v", err)
	}
	userKeyFile, err := ioutil.TempFile("", "userkey")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	if _, err := userKeyFile.Write(userKeyPEM); err != nil {
		t.Fatalf("Failed to write user key: %v", err)
	}
	userKeyFile.Close()

	config := &ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			return nil, nil
		},
	}
	config.AddHostKey(hostKey)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	go func() {
		for {
			nConn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleConnection(t, nConn, config)
		}
	}()

	return listener.Addr().String(), userKeyFile.Name(), func() {
		listener.Close()
		os.Remove(userKeyFile.Name())
	}
}

func handleConnection(t *testing.T, nConn net.Conn, config *ssh.ServerConfig) {
	_, chans, reqs, err := ssh.NewServerConn(nConn, config)
	if err != nil {
		return
	}
	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}

		go func(in <-chan *ssh.Request) {
			for req := range in {
				switch req.Type {
				case "exec":
					cmd := string(req.Payload[4:])
					var status struct{ Status uint32 }
					switch cmd {
					case "exit 1":
						status.Status = 1
					case "writeerr":
						channel.Stderr().Write([]byte("boom"))
						status.Status = 2
					default:
						channel.Write([]byte("output"))
						status.Status = 0
					}
					req.Reply(true, nil)
					channel.SendRequest("exit-status", false, ssh.Marshal(&status))
					channel.Close()
				}
			}
		}(requests)
	}
}

func TestSSHCommandBool(t *testing.T) {
	addr, keyFile, cleanup := startSSHServer(t)
	defer cleanup()

	host, port, _ := net.SplitHostPort(addr)
	sci := ServerConnInfo{
		Server: host,
		Port:   port,
		User:   "testuser",
		Key:    keyFile,
	}

	// Test success
	success, err := SSHCommandBool("echo hello", sci)
	if err != nil {
		t.Errorf("SSHCommandBool failed: %v", err)
	}
	if !success {
		t.Error("SSHCommandBool expected success, got failure")
	}

	// Test failure
	success, err = SSHCommandBool("exit 1", sci)
	if err == nil && success {
		t.Error("SSHCommandBool expected failure, got success")
	}
}

func TestSSHCommandString(t *testing.T) {
	addr, keyFile, cleanup := startSSHServer(t)
	defer cleanup()

	host, port, _ := net.SplitHostPort(addr)
	sci := ServerConnInfo{
		Server: host,
		Port:   port,
		User:   "testuser",
		Key:    keyFile,
	}

	output, err := SSHCommandString("echo hello", sci)
	if err != nil {
		t.Errorf("SSHCommandString failed: %v", err)
	}
	if output != "output" {
		t.Errorf("SSHCommandString expected 'output', got '%s'", output)
	}
}

func TestSSHCommandStringStderr(t *testing.T) {
	addr, keyFile, cleanup := startSSHServer(t)
	defer cleanup()

	host, port, _ := net.SplitHostPort(addr)
	sci := ServerConnInfo{Server: host, Port: port, User: "testuser", Key: keyFile}

	_, err := SSHCommandString("writeerr", sci)
	if err == nil {
		t.Fatal("expected an error for a non-zero remote command")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("expected remote stderr 'boom' to be surfaced in the error, got: %v", err)
	}
}

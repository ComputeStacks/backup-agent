// https://gist.github.com/josephspurrier/a9ab3a1eb68d514a1f7c
package sshremote

/*
// Example
sci := ServerConnInfo{
	"127.0.0.1",
	"22",
	"ubuntu",
	`key.pem`,
}

//command := "sudo apt-get install zip"
//command := "sudo apt-get update"
//command := "sudo apt-get update"
// Count the number of logged in users
command := "who | wc -l"

success, exitError := SSHCommandBool(command, sci)
log.Println("Success", success)
log.Println("Error", exitError)

output, exitError := SSHCommandString(command, sci)
log.Println("Result", output)
log.Println("Error", exitError)
*/

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"

	"golang.org/x/crypto/ssh"
)

type ServerConnInfo struct {
	Server string
	Port   string
	User   string
	Key    string
}

func (c *ServerConnInfo) Socket() string {
	return fmt.Sprintf("%s:%s", c.Server, c.Port)
}

func publicKeyFile(file string) (ssh.AuthMethod, error) {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	key, err := ssh.ParsePrivateKey(buffer)
	if err != nil {
		return nil, err
	}
	return ssh.PublicKeys(key), nil
}

func generateSession(s ServerConnInfo) (*ssh.Session, ssh.Conn, error) {
	publicKey, err := publicKeyFile(s.Key)
	if err != nil {
		return nil, nil, err
	}

	config := &ssh.ClientConfig{
		User: s.User,
		Auth: []ssh.AuthMethod{
			publicKey,
		},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}

	conn, err := ssh.Dial("tcp", s.Socket(), config)
	if err != nil {
		return nil, nil, err
	}

	// Each ClientConn can support multiple interactive sessions,
	// represented by a Session.
	session, err := conn.NewSession()
	if err != nil {
		return nil, conn, err
	}

	return session, conn, nil
}

func SSHCommandBool(command string, sci ServerConnInfo) (bool, error) {
	session, conn, err := generateSession(sci)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}

		return false, err
	}

	session.Stdout = os.Stdout
	session.Stderr = os.Stderr

	err = session.Run(command)

	_ = session.Close()
	_ = conn.Close()

	if err != nil {
		return false, err
	}
	return true, nil
}

func SSHCommandString(command string, sci ServerConnInfo) (string, error) {
	session, conn, err := generateSession(sci)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}

		return "", err
	}

	var stdoutBuf bytes.Buffer
	session.Stdout = &stdoutBuf

	err = session.Run(command)

	_ = session.Close()
	_ = conn.Close()

	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(stdoutBuf.String(), "\n"), nil
}

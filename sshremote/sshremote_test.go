package sshremote

import (
	"os"
	"testing"
)

var SSHPrivateKey string

func TestMain(m *testing.M) {
	SSHPrivateKey = os.Getenv("AGENT_SSH_KEY_FILE")
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestSSHCommandBool(t *testing.T) {

	connInfo := ServerConnInfo{
		Server: "192.168.173.10",
		Port: "22",
		User: "vagrant",
		Key: SSHPrivateKey,
	}

	success, exitError := SSHCommandBool("date", connInfo)

	if exitError != nil {
		t.Errorf("Fatal error: %s", exitError.Error())
		return
	}

	if !success {
		t.Error("Expected success to be ture")
	}
	return

}

func TestSSHCommandString(t *testing.T) {

	connInfo := ServerConnInfo{
		Server: "192.168.173.10",
		Port: "22",
		User: "vagrant",
		Key: SSHPrivateKey,
	}

	output, exitError := SSHCommandString("whoami", connInfo)

	if exitError != nil {
		t.Errorf("Fatal error: %s", exitError.Error())
		return
	}

	if output != "vagrant" {
		t.Errorf("Expected user vagrant, got %s", output)
	}

}

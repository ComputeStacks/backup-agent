package log

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestNew(t *testing.T) {
	test := New().Named("test")
	test.Warn("This is a test message")
}

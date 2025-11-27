package config

import (
	"testing"

	"github.com/spf13/viper"
)

func TestConfigureAppDefaults(t *testing.T) {
	// Reset viper
	viper.Reset()

	ConfigureApp()

	tests := []struct {
		key      string
		expected interface{}
	}{
		{"log.level", "INFO"},
		{"docker.version", "1.44"},
		{"queue.numworkers", 3},
		{"backups.enabled", true},
		{"backups.borg.image", "ghcr.io/computestacks/cs-docker-borg:latest"},
	}

	for _, tt := range tests {
		val := viper.Get(tt.key)
		if val != tt.expected {
			t.Errorf("Expected %s to be %v, got %v", tt.key, tt.expected, val)
		}
	}
}

func TestReleaseEnvironment(t *testing.T) {
	tests := []struct {
		backupKey string
		expected  string
	}{
		{"changeme!", "development"},
		{"tester!", "testing"},
		{"somesecurekey", "production"},
	}

	for _, tt := range tests {
		viper.Set("backups.key", tt.backupKey)
		got := ReleaseEnvironment()
		if got != tt.expected {
			t.Errorf("With backups.key=%s, expected %s, got %s", tt.backupKey, tt.expected, got)
		}
	}
}

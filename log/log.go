package log

import (
	"github.com/hashicorp/go-hclog"
	"github.com/spf13/viper"
)

// New builds a generic logger interface
func New() hclog.Logger {
	var lvl string
	switch viper.GetString("backups.key") {
	case "changeme!", "tester!":
		lvl = "DEBUG"
	default:
		lvl = "INFO"
	}
	return hclog.New(&hclog.LoggerOptions{
		Name:       "cs-agent",
		Level:      hclog.LevelFromString(lvl),
		TimeFormat: "2006/01/02 15:04:05",
	})
}

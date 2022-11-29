package log

import (
	"github.com/hashicorp/go-hclog"
	"github.com/spf13/viper"
)

// New builds a generic logger interface
func New() hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:       "cs-agent",
		Level:      hclog.LevelFromString(viper.GetString("log.level")),
		TimeFormat: "2006/01/02 15:04:05",
	})
}

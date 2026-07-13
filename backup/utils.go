package backup

import (
	"cs-agent/log"

	"github.com/hashicorp/go-hclog"
)

func backupLogger() hclog.Logger {
	return log.New().Named("backup")
}

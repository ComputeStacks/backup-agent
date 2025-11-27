package backup

import (
	"cs-agent/log"
	"cs-agent/types"

	"github.com/hashicorp/go-hclog"
)

// Delete a path in Consul (not a tree).
// Delete a path in Consul (not a tree).
func consulDeletePath(consul types.ConsulKV, path string) {
	_, _ = consul.Delete(path, nil)
}

func backupLogger() hclog.Logger {
	return log.New().Named("backup")
}

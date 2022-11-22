package backup

import (
	"cs-agent/log"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
	"runtime"
	"strings"
)

// Delete a path in Consul (not a tree).
func consulDeletePath(consul *consulAPI.Client, path string) {
	kv := consul.KV()
	_,_ = kv.Delete(path, nil)
}

func backupLogger() hclog.Logger {
	return log.New().Named("backup")
}

func CurrentPath() string {
	// Configure our path
	_, filename, _, _ := runtime.Caller(0)
	p := strings.Split(filename, "/")
	p = p[:len(p)-1]
	return strings.Join(p, "/")
}
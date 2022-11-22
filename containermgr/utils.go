package containermgr

import (
	"cs-agent/log"

	"github.com/hashicorp/go-hclog"
)

func containerLogger() hclog.Logger {
	return log.New().Named("containermgr")
}

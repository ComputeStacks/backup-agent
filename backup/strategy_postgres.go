package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/containermgr"
	"cs-agent/csevent"
	"cs-agent/types"
	"github.com/docker/docker/client"
	"github.com/spf13/viper"
	"strconv"
)

func preBackupPostgres(vol *types.Volume, event *csevent.ProjectEvent) (preBackupPostgresSuccess bool) {
	cli, err := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if err != nil {
		backupLogger().Warn("Docker error preBackupPostgres", "error", err.Error())
		go event.PostEventUpdate("agent-88a614c7c22772e5", "Fatal error connecting to docker for preBackupPostgres Job")
		return false
	}
	c, err := containermgr.FindByService(cli, strconv.Itoa(vol.ServiceID), false)

	// If container is offline, then we don't need to do anything! allow normal file-level backup to continue
	if err != nil {
		return true
	}
	exitCode, _, err := c.Exec([]string{"psql", "-U", "postgres", "-c", "checkpoint;"}, event)

	if exitCode > 0 {
		backupLogger().Warn("Failed to run preBackupPostgres Job", "exitCode", exitCode)
		go event.PostEventUpdate("agent-dc7bfd8aa3bb1042", "Failed to run preBackupPostgres Job")
		return false
	}

	return true
}

func postBackupPostgres(event *csevent.ProjectEvent, repo *borg.Repository) bool {
	return true
}

func preRestorePostgres(vol *types.Volume, event *csevent.ProjectEvent, repo *borg.Repository) (preRestorePostgresSuccess bool) {
	return true
}

func postRestorePostgres(event *csevent.ProjectEvent, repo *borg.Repository) bool {
	return true
}

func rollbackRestorePostgres(event *csevent.ProjectEvent, repo *borg.Repository) bool {
	return true
}

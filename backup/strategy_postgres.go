package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/containermgr"
	"cs-agent/types"
	"strconv"

	"github.com/docker/docker/client"
	"github.com/spf13/viper"
)

func preBackupPostgres(vol *types.Volume, event *progress) (preBackupPostgresSuccess bool) {
	cli, err := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if err != nil {
		backupLogger().Warn("Docker error preBackupPostgres", "error", err.Error())
		event.PostEventUpdate("agent-88a614c7c22772e5", "Fatal error connecting to docker for preBackupPostgres Job")
		return false
	}
	c, err := containermgr.FindByService(cli, strconv.Itoa(vol.ServiceID), false)

	// If container is offline, then we don't need to do anything! allow normal file-level backup to continue
	if err != nil {
		return true
	}
	exitCode, out, err := c.Exec([]string{"psql", "-U", "postgres", "-c", "checkpoint;"})
	if err != nil {
		backupLogger().Warn("Failed to run preBackupPostgres exec", "error", err.Error())
		event.PostEventUpdate("agent-dc7bfd8aa3bb1042", withOutput("Failed to run preBackupPostgres Job: "+err.Error(), out))
		return false
	}

	if exitCode > 0 {
		backupLogger().Warn("Failed to run preBackupPostgres Job", "exitCode", exitCode)
		event.PostEventUpdate("agent-dc7bfd8aa3bb1042", withOutput("Failed to run preBackupPostgres Job", out))
		return false
	}

	return true
}

func postBackupPostgres(event *progress, repo *borg.Repository) bool {
	return true
}

func preRestorePostgres(vol *types.Volume, event *progress, repo *borg.Repository) (preRestorePostgresSuccess bool) {
	return true
}

func postRestorePostgres(event *progress, repo *borg.Repository) bool {
	return true
}

func rollbackRestorePostgres(event *progress, repo *borg.Repository) bool {
	return true
}

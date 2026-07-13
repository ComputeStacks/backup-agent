package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/containermgr"
	"cs-agent/types"
	"strconv"
	"strings"

	"github.com/docker/docker/client"
	"github.com/spf13/viper"
)

func preRestoreMysql(vol *types.Volume, event *progress, repo *borg.Repository) (preRestoreMysqlSuccess bool) {

	if !stopAllMysqlContainers(vol, event) {
		return false
	}

	// Delete existing data
	var preRestoreCmd []string
	var execCmd []string
	preRestoreCmd = append(preRestoreCmd, "mkdir", "-p /root/.snapshot")
	preRestoreCmd = append(preRestoreCmd, "&&", "mv", "/mnt/data/* /root/.snapshot/")

	execCmd = append(execCmd, "sh", "-c", strings.Join(preRestoreCmd, " "))

	exitCode, out, err := repo.Container.Exec(execCmd)

	if err != nil {
		backupLogger().Warn("Failed to snapshot existing data", "error", err.Error())
		event.PostEventUpdate("agent-82c8d22caa01995d", withOutput(err.Error(), out))
		return false
	}

	if exitCode > 0 {
		backupLogger().Warn("Failed to run preRestoreMysql Job", "exitCode", exitCode, "commands", "backupCmd")
		event.PostEventUpdate("agent-0590433e5ef199c9", withOutput("Save data command failed to run.", out))
		return false
	}

	return true
}

func postRestoreMysql(event *progress, repo *borg.Repository) bool {

	var postRestoreCmd []string
	var execCmd []string
	postRestoreCmd = append(postRestoreCmd, "mkdir", "-p /root/.staging")
	postRestoreCmd = append(postRestoreCmd, "&&", "mv", "/mnt/data/* /root/.staging/")
	postRestoreCmd = append(postRestoreCmd, "&&", "rm", "-rf /mnt/data/*")
	postRestoreCmd = append(postRestoreCmd, "&&", "mv", "/root/.staging/backups/* /mnt/data/")

	execCmd = append(execCmd, "sh", "-c", strings.Join(postRestoreCmd, " "))

	exitCode, out, err := repo.Container.Exec(execCmd)

	if err != nil {
		backupLogger().Warn("Failed to execute mysql cleanup on restore", "error", err.Error())
		event.PostEventUpdate("agent-c24b0abcd88acdff", withOutput(err.Error(), out))
		return false
	}
	if exitCode > 0 {
		event.PostEventUpdate("agent-c24b0abcd88acdff", withOutput("postRestoreMysql cleanup returned a non-zero exit code", out))
		return false
	}

	return true

}

func rollbackRestoreMysql(event *progress, repo *borg.Repository) bool {

	// Clean MySQL directory and move files back
	var rollbackCmd []string
	var execCmd []string
	rollbackCmd = append(rollbackCmd, "rm", "-rf /mnt/data/*")
	rollbackCmd = append(rollbackCmd, "&&", "mv", "/root/.snapshot/* /mnt/data/")
	rollbackCmd = append(rollbackCmd, "&&", "rm", "-rf /mnt/data/backups")

	execCmd = append(execCmd, "sh", "-c", strings.Join(rollbackCmd, " "))

	exitCode, out, err := repo.Container.Exec(execCmd)

	if err != nil {
		backupLogger().Warn("Failed to store database backup", "error", err.Error())
		event.PostEventUpdate("agent-af1b0badd5d9b9f6", withOutput(err.Error(), out))
		return false
	}
	if exitCode > 0 {
		event.PostEventUpdate("agent-af1b0badd5d9b9f6", withOutput("rollbackRestoreMysql returned a non-zero exit code", out))
		return false
	}

	return true

}

func stopAllMysqlContainers(vol *types.Volume, event *progress) bool {
	cli, cliErr := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if cliErr != nil {
		backupLogger().Warn("Failed to connect to docker", "error", cliErr.Error(), "function", "stopAllMysqlContainers")
		event.PostEventUpdate("agent-45a73ed06ea34e25", cliErr.Error())
		return false
	}
	containers, findAllErr := containermgr.FindAllByService(cli, strconv.Itoa(vol.ServiceID), true)
	if findAllErr != nil {
		backupLogger().Warn("Failed to retrieve containers", "error", findAllErr.Error(), "function", "stopAllMysqlContainers")
		event.PostEventUpdate("agent-0248a778f49a1eb4", findAllErr.Error())
		return false
	}
	failedToStop := false
	for _, c := range containers {
		if !c.Stop() {
			failedToStop = true
		}
	}
	if failedToStop {
		event.PostEventUpdate("agent-6becd55bb6a584de", "Failed to stop some containers, unable to restore.")
		for _, c := range containers {
			_ = c.Start() // Ignore containers that fail to start
		}
		return false
	}
	return true
}

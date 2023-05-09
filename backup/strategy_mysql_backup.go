package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/containermgr"
	"cs-agent/csevent"
	"cs-agent/types"
	"strconv"
	"strings"

	"github.com/docker/docker/client"
	"github.com/spf13/viper"
)

func preBackupMysql(vol *types.Volume, event *csevent.ProjectEvent) bool {

	// First, locate a running container
	cli, err := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if err != nil {
		backupLogger().Warn("Skipping mysql backup due to docker error", "error", err.Error())
		return false
	}
	mysqlMaster, findMysqlErr := loadMysqlMaster(cli, strconv.Itoa(vol.ServiceID), event, false)

	if findMysqlErr != nil {
		return false
	}

	/**
	We build and run the image like a normal container, rather than a "one-shot" command, so that we can re-use our existing
	container exec code and capture logging back to CS.
	*/
	var container *containermgr.Container
	if mysqlMaster.Variant == "mariadb" {
		container = mysqlMaster.Container
	} else {
		var err error
		container, err = buildBackupAgent(cli, mysqlMaster)
		defer container.Stop()

		if err != nil {
			backupLogger().Warn("Failed to create backup container", "error", err.Error())
			go event.PostEventUpdate("agent-45b31fb5d2814cd8", "Failed to create backup container: "+err.Error())
			return false
		}
	}

	isReadyResult := isMysqlReady(container, mysqlMaster, event)

	if !isReadyResult {
		return false
	}

	backupMysqlResult := backupMysql(container, mysqlMaster, event)

	if !backupMysqlResult {
		return false
	}

	return prepareMysqlBackup(container, mysqlMaster, event)
}

func postBackupMysql(event *csevent.ProjectEvent, repo *borg.Repository) bool {

	var postBackupCmd []string
	var execCmd []string
	postBackupCmd = append(postBackupCmd, "rm -rf", "/mnt/data/backups")
	execCmd = append(execCmd, "sh", "-c", strings.Join(postBackupCmd, " "))

	exitCode, _, err := repo.Container.Exec(execCmd, event)

	if err != nil {
		go event.PostEventUpdate("agent-eb2b4ef10b08d3d5", err.Error())
		return false
	}

	return exitCode == 0
}

/*
	MySQL Backup Commands
*/

// Determine if MariaDB / MySQL is ready to perform a backup
func isMysqlReady(backupContainer *containermgr.Container, mysqlMaster *MysqlInstance, event *csevent.ProjectEvent) bool {
	var readyCmd []string
	var execCmd []string
	readyCmd = append(readyCmd, "counter=0; while ! mysql -h", mysqlMaster.IPAddress, "-uroot", "-p"+mysqlMaster.Password, "-e \"STATUS;\"", "&>/dev/null; do counter=$((counter+1)); if [ $counter == 11 ]; then echo 'Failed to connect'; exit 1; fi; echo \"Connection attempt $counter\"; sleep 5; done;")
	readyCmd = append(readyCmd, "if [ -d "+mysqlMaster.DataPath+"/backups ]; then rm -rf "+mysqlMaster.DataPath+"/backups; fi") // Ensure the backups dir does not exist, otherwise backups will fail.

	execCmd = append(execCmd, "bash", "-c", strings.Join(readyCmd, " "))

	exitCode, _, err := backupContainer.Exec(execCmd, event)

	if err != nil {
		backupLogger().Warn("Failed to create backup container exec", "error", err.Error())
		go event.PostEventUpdate("agent-a3171394f4f28d20", "Failed run isMysqlReady check command: "+err.Error())
		return false
	}

	if exitCode > 0 {
		backupLogger().Warn("Failed to run isMysqlReady command", "exitCode", exitCode, "commands", strings.Join(readyCmd, " "))
		go event.PostEventUpdate("agent-b3dae7aba0783df4", "Failed to complete preBackupMysql Job. Halted during isMysqlReady.")
		return false
	}

	return true
}

func backupMysql(backupContainer *containermgr.Container, mysqlMaster *MysqlInstance, event *csevent.ProjectEvent) bool {
	var backupCmd []string

	backupExec := "xtrabackup"

	if mysqlMaster.Variant == "mariadb" || mysqlMaster.Variant == "bitnami-mariadb" {
		backupExec = "mariabackup"
	}

	mariaLongTimeout := viper.GetString("mariadb.long_queries.timeout")
	mariaLongType := viper.GetString("mariadb.long_queries.query_type")
	mariaWaitQueryType := viper.GetString("mariadb.lock_wait.query_type")
	mariaWaitTimeout := viper.GetString("mariadb.lock_wait.timeout")

	backupCmd = append(backupCmd, backupExec, "--backup", "--datadir="+mysqlMaster.DataPath, "--port=3306")
	backupCmd = append(backupCmd, "--target-dir="+mysqlMaster.DataPath+"/backups")
	backupCmd = append(backupCmd, "--user="+mysqlMaster.Username)
	backupCmd = append(backupCmd, "--password="+mysqlMaster.Password)
	backupCmd = append(backupCmd, "--host="+mysqlMaster.IPAddress)

	if mysqlMaster.Variant == "mariadb" {
		backupCmd = append(backupCmd, "--ftwrl-wait-query-type="+mariaWaitQueryType)
		backupCmd = append(backupCmd, "--ftwrl-wait-timeout="+mariaWaitTimeout)
		backupCmd = append(backupCmd, "--kill-long-query-type="+mariaLongType)
		backupCmd = append(backupCmd, "--kill-long-queries-timeout="+mariaLongTimeout)
	}

	exitCode, _, err := backupContainer.Exec(backupCmd, event)

	if err != nil {
		backupLogger().Warn("Failed to run backupMysql", "error", err.Error())
		go event.PostEventUpdate("agent-2d2e814ec98a57d4", "Failed to run backup mysql: "+err.Error())
		return false
	}

	if exitCode > 0 {
		backupLogger().Warn("Failed to run backupMysql Job", "exitCode", exitCode, "commands", strings.Join(backupCmd, " "))
		go event.PostEventUpdate("agent-40840727428848c7", "Failed to run preBackupMysql Job due to backupMysql error.")
		return false
	}

	return true
}

func prepareMysqlBackup(backupContainer *containermgr.Container, mysqlMaster *MysqlInstance, event *csevent.ProjectEvent) bool {
	var backupPrepareCmd []string

	backupExec := "xtrabackup"

	if mysqlMaster.Variant == "mariadb" || mysqlMaster.Variant == "bitnami-mariadb" {
		backupExec = "mariabackup"
	}

	backupPrepareCmd = append(backupPrepareCmd, backupExec, "--prepare", "--target-dir="+mysqlMaster.DataPath+"/backups")

	exitCode, _, err := backupContainer.Exec(backupPrepareCmd, event)

	if err != nil {
		backupLogger().Warn("Failed to run prepareMysqlBackup", "error", err.Error())
		go event.PostEventUpdate("agent-7dfa6f8829fb8c7e", "Failed to prepare mysql backup: "+err.Error())
		return false
	}

	if exitCode > 0 {
		backupLogger().Warn("Failed to run backupMysql Job", "exitCode", exitCode, "commands", strings.Join(backupPrepareCmd, " "))
		go event.PostEventUpdate("agent-2fe3bce7adbb0f58", "Failed to run prepareMysqlBackup Job due to backupMysql error.")
		return false
	}

	return true
}

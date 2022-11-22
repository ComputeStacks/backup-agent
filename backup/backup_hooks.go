package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/containermgr"
	"cs-agent/csevent"
	"cs-agent/types"
	"cs-agent/utils"
	"reflect"
	"strconv"
	"strings"
)

func preBackup(vol *types.Volume, event *csevent.ProjectEvent) (preBackupSuccess bool) {
	backupLogger().Info("Running preBackup job", "volume", vol.Name, "strategy", vol.Strategy)
	if vol.Strategy == "" {
		backupLogger().Debug("Strategy is blank for volume, defaulting to files.", "volume", vol.Strategy)
	}
	if len(vol.PreBackup) > 2 {
		success := false
		defer func() (preBackupSuccess bool) {
			if r := recover(); r != nil {
				r := reflect.ValueOf(r)
				go event.PostEventUpdate("agent-98840575b7f423c4", utils.RecoverErrorToString(r))
				return false
			}
			return success
		}()
		exitCode, _, err := containermgr.ServiceExec(strconv.Itoa(vol.ServiceID), vol.PreBackup, event)

		if err != nil {
			go event.PostEventUpdate("agent-ce329a15239aeb9d", err.Error())
			return false
		}

		if exitCode > 0 {
			if vol.BackupContinueOnError {
				finalMsg := "Pre-Backup command returned a non-zero exit code (" + string(rune(exitCode)) + "): " + strings.Join(vol.PreBackup, " ")
				go event.PostEventUpdate("agent-0fe15b3798ec8dfd", finalMsg)
			} else {
				finalMsg := "Backup halted due to non-zero exit code (" + string(rune(exitCode)) + "): " + strings.Join(vol.PreBackup, " ")
				go event.PostEventUpdate("agent-5594abeced0476b3", finalMsg)
				return false
			}
		}

	}
	switch vol.Strategy {
	case "mysql":
		vol.BackupContinueOnError = true // Force this to be true, otherwise we will have stale backup containers running.
		return preBackupMysql(vol, event)
	case "postgres":
		vol.BackupContinueOnError = true // Force this to be true, otherwise we will have stale backup containers running.
		return preBackupPostgres(vol, event)
	default:
		return true
	}
}

func postBackup(vol *types.Volume, event *csevent.ProjectEvent, repo *borg.Repository) {

	backupLogger().Info("Running postBackup", "volume", vol.Name)

	if len(vol.PostBackup) > 2 {
		defer func() {
			if r := recover(); r != nil {
				r := reflect.ValueOf(r)
				go event.PostEventUpdate("agent-b400393269184c12", utils.RecoverErrorToString(r))
				return
			}
		}()
		exitCode, _, err := containermgr.ServiceExec(strconv.Itoa(vol.ServiceID), vol.PostBackup, event)

		if err != nil {
			go event.PostEventUpdate("agent-ee979a773f9b7788", err.Error())
			return
		}

		if exitCode > 0 {
			finalMsg := "Post-Backup commands returned a non-zero exit code (" + string(rune(exitCode)) + "): " + strings.Join(vol.PostBackup, " ")
			go event.PostEventUpdate("agent-bb7c6f21b16ac27b", finalMsg)
		}

	}
	switch vol.Strategy {
	case "mysql":
		_ = postBackupMysql(event, repo)
	case "postgres":
		_ = postBackupPostgres(event, repo)
	default:
		return
	}
}

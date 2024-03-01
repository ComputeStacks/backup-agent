// Package backup
/*
# Restore Hooks

preRestore will run before the backup is restored to the server, and before the container is
powered off.

postRestore will run after the backup is transferred to the container, but before the container
is booted back up.
*/
package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/containermgr"
	"cs-agent/csevent"
	"cs-agent/types"
	"fmt"
	"strconv"
	"strings"
)

func preRestore(vol *types.Volume, event *csevent.ProjectEvent, repo *borg.Repository) (preRestoreSuccess bool) {

	if len(vol.PreRestore) > 2 {
		success := false
		defer func() (preRestoreSuccess bool) {
			if r := recover(); r != nil {
				go event.PostEventUpdate("agent-cd090c1cc5c19617", fmt.Sprintf("%#v", r))
				return false
			}
			return success
		}()
		exitCode, _, err := containermgr.ServiceExec(strconv.Itoa(vol.ServiceID), vol.PreRestore, event)

		if err != nil {
			go event.PostEventUpdate("agent-b98d45dff8fd639b", err.Error())
			return false
		}

		if exitCode > 0 {
			if vol.RestoreContinueOnError {
				finalMsg := "Pre-Restore command returned a non-zero exit code (" + string(rune(exitCode)) + "): " + strings.Join(vol.PreRestore, " ")
				go event.PostEventUpdate("agent-17a5e40308439ab3", finalMsg)
			} else {
				finalMsg := "Restored halted due to non-zero exit code (" + string(rune(exitCode)) + "): " + strings.Join(vol.PreRestore, " ")
				go event.PostEventUpdate("agent-059e93c9920612b8", finalMsg)
				return false
			}
		}

	}

	switch vol.Strategy {
	case "mysql":
		return preRestoreMysql(vol, event, repo)
	case "postgres":
		return preRestorePostgres(vol, event, repo)
	default:
		return true
	}

}

func postRestore(vol *types.Volume, event *csevent.ProjectEvent, repo *borg.Repository) bool {

	if len(vol.PostRestore) > 2 {
		defer func() bool {
			if r := recover(); r != nil {
				go event.PostEventUpdate("agent-b5117962943e98cb", fmt.Sprintf("%#v", r))
				return false
			}
			return true
		}()
		exitCode, _, err := containermgr.ServiceExec(strconv.Itoa(vol.ServiceID), vol.PostRestore, event)

		if err != nil {
			go event.PostEventUpdate("agent-1b5d010969199a18", err.Error())
			return false
		}

		if exitCode > 0 {
			finalMsg := "Post-Backup commands returned a non-zero exit code (" + string(rune(exitCode)) + "): " + strings.Join(vol.PostRestore, " ")
			go event.PostEventUpdate("agent-9330f19b388b4428", finalMsg)
			return false
		}

	}
	switch vol.Strategy {
	case "mysql":
		return postRestoreMysql(event, repo)
	case "postgres":
		return postRestorePostgres(event, repo)
	default:
		return true
	}
}

func rollbackRestore(vol *types.Volume, event *csevent.ProjectEvent, repo *borg.Repository) bool {

	if len(vol.PostRestore) > 0 {
		defer func() bool {
			if r := recover(); r != nil {
				go event.PostEventUpdate("agent-c290fcc106e4f78a", fmt.Sprintf("%#v", r))
				return false
			}
			return true
		}()
		exitCode, _, err := containermgr.ServiceExec(strconv.Itoa(vol.ServiceID), vol.PostRestore, event)

		if err != nil {
			go event.PostEventUpdate("agent-9393516879f411ea", err.Error())
			return false
		}

		if exitCode > 0 {
			finalMsg := "Post-Backup commands returned a non-zero exit code (" + string(rune(exitCode)) + "): " + strings.Join(vol.PostRestore, " ")
			go event.PostEventUpdate("agent-cf02d05dd4d77905", finalMsg)
			return false
		}

	}
	switch vol.Strategy {
	case "mysql":
		return rollbackRestoreMysql(event, repo)
	case "postgres":
		return rollbackRestorePostgres(event, repo)
	default:
		return true
	}
}

/*
*
Cron scheduling and setup for Volume Backups
*/
package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/csevent"
	"cs-agent/types"
	"errors"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
)

func Perform(consul *consulAPI.Client, job *types.Job) error {
	defer sentry.Recover()
	hostname, _ := os.Hostname()
	kv := consul.KV()
	opts := &consulAPI.QueryOptions{RequireConsistent: true}
	data, _, err := kv.Get("volumes/"+job.VolumeName, opts)
	if err != nil {
		backupLogger().Warn("Fatal error loading volume from consul", "volume", job.VolumeName, "error", err.Error())
		sentry.CaptureException(err)
		return err
	}

	if data == nil {
		backupLogger().Warn("Skipping backup job for unknown volume", "volume", job.VolumeName)
		return nil
	}
	backupLogger().Info("Performing volume backup", "volume", job.VolumeName)
	vol, err := types.LoadVolume(data.Value)

	if err != nil {
		backupLogger().Warn("Fatal error loading volume", "volume", data.Value, "error", err.Error())
		sentry.CaptureException(err)
		return err
	}

	if vol.Node != hostname {
		backupLogger().Info("Skipping backup job for volume not under my control", "volume", job.VolumeName)
		return nil
	}

	backupLogger().Info("Backing up volume", "volume", job.VolumeName)
	volumeIds := []int{vol.ID}
	backupLogger().Info("Building project event", "volume", vol.Name, "project", vol.ProjectID)
	projectEvent := csevent.New(vol.ProjectID, volumeIds, "agent-ad28e9aa1933495f", "volume.backup", job.AuditID)

	defer func() {
		if projectEvent != nil {
			projectEvent.CloseEvent()
		}
	}()

	repo, findRepoMsg := borg.FindRepository(&vol, &vol)

	defer func() {
		// Stop borg container
		if !repo.StopContainer() {
			projectEvent.PostEventUpdate("agent-fe161b668b0756df", "Failed to stop backup container")
		}
	}()

	if findRepoMsg != nil {
		if findRepoMsg.MsgID == "Repository.DoesNotExist" {
			repo = &borg.Repository{Name: vol.Name}
			// Build backup container
			repoErr := repo.Setup(&vol, &vol)
			if repoErr != nil && projectEvent != nil {
				projectEvent.EventLog.Status = "failed"
				projectEvent.PostEventUpdate("agent-d4c34f1d89c20aa6", repoErr.ToYaml())
				projectEvent.CloseEvent()
				return errors.New(repoErr.Message)
			}
		} else {
			if projectEvent != nil {
				projectEvent.EventLog.Status = "failed"
				projectEvent.PostEventUpdate("agent-c4087f229d50d4dc", findRepoMsg.ToYaml())
				projectEvent.CloseEvent()
			}
			return errors.New("(" + findRepoMsg.MsgID + ") " + findRepoMsg.Message)
		}
	}

	archive := borg.Archive{
		Name:       job.ArchiveName,
		Repository: repo,
	}

	preBackupSuccess := preBackup(&vol, projectEvent)

	if preBackupSuccess {
		archiveMsg, archiveErr := archive.Create()
		if archiveErr != nil {
			if projectEvent != nil {
				projectEvent.PostEventUpdate("agent-d894f86c71d0db7b", archiveErr.ToYaml())
				if projectEvent.EventLog.Status == "running" {
					projectEvent.EventLog.Status = "failed"
				}
			}

			if vol.RestoreContinueOnError {
				postBackup(&vol, projectEvent, repo)
			}
		} else {
			if projectEvent != nil {
				projectEvent.PostEventUpdate("agent-566dd010f637861e", archiveMsg.ToYaml())
			}
			postBackup(&vol, projectEvent, repo)

		}
	} else {
		if projectEvent != nil {
			if projectEvent.EventLog.Status == "running" {
				projectEvent.EventLog.Status = "failed"
			}
		}

		if vol.BackupContinueOnError {
			postBackup(&vol, projectEvent, repo)
		}
	}

	// Set last backup time to now.
	vol.LastBackup = int64(time.Now().Unix())
	data.Value = vol.JSONEncode()
	_, kvError := kv.Put(data, nil)
	if kvError != nil {
		backupLogger().Warn("Fatal error loading volume", "volume", vol.Name, "error", kvError.Error())
		sentry.CaptureException(kvError)
		return kvError
	}
	return nil

}

package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/csevent"
	"cs-agent/types"
	"errors"
	"os"
	"strings"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
)

func DeleteBackup(consul *consulAPI.Client, job *types.Job) error {
	hostname, _ := os.Hostname()
	kv := consul.KV()
	opts := &consulAPI.QueryOptions{RequireConsistent: true}
	data, _, err := kv.Get("volumes/"+job.VolumeName, opts)
	if err != nil {
		backupLogger().Warn("Error loading consul", "function", "DeleteBackup()", "error", err.Error())
		return err
	}

	if data == nil {
		backupLogger().Info("Missing volume", "function", "DeleteBackup()", "error", "No data")
		return nil
	}

	vol, err := types.LoadVolume(data.Value)

	if err != nil {
		backupLogger().Warn("Fatal error parsing volume from consul data", "volume", data.Value, "error", err.Error())
		sentry.CaptureException(err)
		return err
	}

	if vol.Node != hostname {
		backupLogger().Info("Skipping backup deletion", "function", "DeleteBackup()", "error", "Not assigned to me.", "assignedTo", vol.Node)
		return nil
	}

	projectEvent := csevent.New(vol.ProjectID, []int{vol.ID}, "agent-1105683bb0f948c0", "backup.delete", job.AuditID)

	repo, findRepoErr := borg.FindRepository(&types.Volume{Name: job.VolumeName}, &types.Volume{Name: job.SourceVolumeName})

	if findRepoErr != nil {
		projectEvent.EventLog.Status = "failed"
		if findRepoErr.Message == "" {
			projectEvent.PostEventUpdate("agent-18e788c90cc0760d", "Failed to find backup repository.")
		} else {
			projectEvent.PostEventUpdate("agent-55f82aabccdb8643", findRepoErr.Message)
		}
		projectEvent.CloseEvent()
		backupLogger().Warn("Failed to find repository", "name", job.VolumeName, "archive", job.ArchiveName, "function", "DeleteBackup", "error", findRepoErr.Message)
		return errors.New("(" + findRepoErr.MsgID + ") " + findRepoErr.Message)
	}

	archive, findArchiveErr := repo.FindArchive(job.ArchiveName)

	if findArchiveErr != nil {
		backupLogger().Warn("Error deleting repository", "volume", vol.Name, "response", findArchiveErr.Message)
		repo.StopContainer()
		projectEvent.EventLog.Status = "failed"
		projectEvent.PostEventUpdate("agent-e67edd61abe38301", findArchiveErr.ToYaml())
		projectEvent.CloseEvent()
		return errors.New("(" + findArchiveErr.MsgID + ") " + findArchiveErr.Message)
	}

	deleteResponse, deleteArchiveErr := archive.Delete()
	repo.StopContainer()

	if deleteArchiveErr != nil {
		backupLogger().Warn("Error deleting archive", "volume", vol.Name, "archive", archive.Name, "response", deleteArchiveErr.Message)
		projectEvent.EventLog.Status = "failed"
		projectEvent.PostEventUpdate("agent-a7ae639c559b3088", deleteArchiveErr.ToYaml())
		projectEvent.CloseEvent()
		return errors.New("(" + deleteArchiveErr.MsgID + ") " + deleteArchiveErr.Message)
	}

	var output []string
	for _, o := range deleteResponse {
		output = append(output, o.Message)
	}
	projectEvent.PostEventUpdate("agent-8f8a9488ed4106f4", strings.Join(output, "\n"))
	projectEvent.CloseEvent()
	return nil
}

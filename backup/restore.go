package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/containermgr"
	"cs-agent/csevent"
	"cs-agent/types"
	"os"
	"strconv"
	"time"

	"github.com/docker/docker/client"
	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/spf13/viper"
)

func Restore(consul *consulAPI.Client, job *types.Job) {
	defer sentry.Recover()
	hostname, _ := os.Hostname()
	kv := consul.KV()
	opts := &consulAPI.QueryOptions{RequireConsistent: true}
	destData, _, destVolErr := kv.Get("volumes/"+job.VolumeName, opts)
	if destVolErr != nil {
		sentry.CaptureException(destVolErr)
		return
	}
	if destData == nil {
		backupLogger().Warn("Skipping restore job for unknown destination volume", "volume", job.VolumeName, "source_volume", job.SourceVolumeName)
		return
	}
	data, _, err := kv.Get("volumes/"+job.SourceVolumeName, opts)
	if err != nil {
		sentry.CaptureException(err)
		return
	}
	if data == nil {
		backupLogger().Warn("Skipping restore job for unknown volume", "volume", job.VolumeName, "source_volume", job.SourceVolumeName)
		return
	}
	backupLogger().Info("Performing volume restore", "volume", job.VolumeName, "source_volume", job.SourceVolumeName)

	// Load Source Volume
	vol, err := types.LoadVolume(data.Value)
	if err != nil {
		backupLogger().Warn("Fatal error parsing volume from consul data", "source_volume", data.Value, "error", err.Error())
		sentry.CaptureException(err)
		return
	}

	// Load Destination Volume
	destVol, destVolErr := types.LoadVolume(destData.Value)
	if destVolErr != nil {
		backupLogger().Warn("Fatal error parsing destination volume from consul data", "volume", destData.Value, "error", err.Error())
		sentry.CaptureException(err)
		return
	}

	// Start restore
	backupLogger().Info("Preparing to restore volume", "volume", job.VolumeName, "source_volume", job.SourceVolumeName)
	volumeIds := []int{destVol.ID}
	projectEvent := csevent.New(destVol.ProjectID, volumeIds, "agent-bde07117ae85937d", "volume.restore", job.AuditID)

	defer projectEvent.CloseEvent()

	// Sanity check to ensure we should perform the backup
	if destVol.Node != hostname {
		backupLogger().Info("Halting restore job because destination volume is not under my control", "volume", job.VolumeName, "source_volume", job.SourceVolumeName)
		projectEvent.PostEventUpdate("agent-806c0cc330c2ea4d", "Halting restore job because destination volume is not under my control.")
		return
	}

	// Sanity check to ensure we should perform the backup
	if !viper.GetBool("backups.borg.nfs") {
		if vol.Node != hostname {
			backupLogger().Info("Halting restore job because source volume is not accessible on host", "volume", job.VolumeName, "source_volume", job.SourceVolumeName)
			projectEvent.PostEventUpdate("agent-bfba3466b03d45e8", "Halting restore job because source volume is not accessible on host.")
			return
		}
	}

	if job.ArchiveName == "" {
		backupLogger().Warn("Error restoring volume, missing archive name", "volume", job.VolumeName, "source_volume", job.SourceVolumeName)
		projectEvent.PostEventUpdate("agent-548b1d752057add0", "Failed to restore volume due to missing backup name.")
		return
	}

	repo, findRepoErr := borg.FindRepository(&destVol, &vol)

	if findRepoErr != nil {
		backupLogger().Warn("Error Restoring volume.", "volume", job.VolumeName, "source_volume", job.SourceVolumeName, "error", findRepoErr.Message)
		projectEvent.PostEventUpdate("agent-81023b3bc0541171", findRepoErr.ToYaml())
		return
	}

	defer repo.StopContainer()

	archive, findArchiveErr := repo.FindArchive(job.ArchiveName)

	if findArchiveErr != nil {
		backupLogger().Warn("Error Restoring volume.", "volume", job.VolumeName, "source_volume", job.SourceVolumeName, "error", findArchiveErr.Message)
		projectEvent.PostEventUpdate("agent-7d32bd2230b39408", findArchiveErr.ToYaml())
		repo.StopContainer()
		return
	}

	cli, restoreDockerErr := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if restoreDockerErr != nil {
		backupLogger().Warn("Failed to connect to docker", "error", restoreDockerErr.Error(), "function", "Restore")
		projectEvent.PostEventUpdate("agent-05297a0a0438a5bf", restoreDockerErr.Error())
		repo.StopContainer()
		return
	}
	containers, findAllErr := containermgr.FindAllByService(cli, strconv.Itoa(destVol.ServiceID), true)

	if findAllErr != nil {
		backupLogger().Warn("Failed to retrieve containers", "error", findAllErr.Error(), "function", "Restore")
		projectEvent.PostEventUpdate("agent-dc1ec275fcf91a6c", findAllErr.Error())
		repo.StopContainer()
		return
	}

	backupLogger().Debug("Running PreRestore hook", "volume", vol.Name)

	preRestoreSuccess := preRestore(&destVol, projectEvent, repo)

	if !preRestoreSuccess {
		repo.StopContainer()
		backupLogger().Warn("Failed to restore volume", "volume", vol.Name, "archive", archive.Name, "error", "PreRestore hook failed.")
		projectEvent.EventLog.Status = "failed"
		projectEvent.CloseEvent()
		return
	}

	failedToStop := false
	for _, c := range containers {
		backupLogger().Debug("Start Restore: Stop Container", "volume", vol.Name, "container", c.ID)
		if !c.Stop() {
			failedToStop = true
			projectEvent.PostEventUpdate("agent-f3ccb16e88d10a80", "Failed to stop container, halting restore process.")
			break
		}
	}

	if failedToStop {
		projectEvent.EventLog.Status = "failed"
		if !rollbackRestore(&destVol, projectEvent, repo) {
			projectEvent.PostEventUpdate("agent-0b33976078a50679", "rollback restore complete successfully.")
		}
	} else {
		// Override file paths for our custom strategies
		switch vol.Strategy {
		case "mysql", "mariadb":
			//job.FilePaths = []string{"backups/"}
			job.FilePaths = []string{}
			break
		case "postgres":
			job.FilePaths = []string{}
			break
		}

		backupLogger().Info("Restoring volume", "source_volume", vol.Name, "volume", destVol.Name, "archive", archive.Name)
		restoreErr := archive.Restore(job.FilePaths)
		if restoreErr != nil {
			projectEvent.PostEventUpdate("agent-6dfe4e7b471fdd4c", restoreErr.ToYaml())
			projectEvent.EventLog.Status = "failed"
			backupLogger().Warn("Failed to restore volume", "source_volume", vol.Name, "volume", destVol.Name, "archive", archive.Name, "error", restoreErr.Message)
			rollbackRestore(&destVol, projectEvent, repo)
		} else {
			if !postRestore(&destVol, projectEvent, repo) {
				projectEvent.PostEventUpdate("agent-12b99684cb30d029", "postRestore failed, executing rollback.")
				if !rollbackRestore(&destVol, projectEvent, repo) {
					projectEvent.PostEventUpdate("agent-b9f3171f4182ee92", "rollback restore complete successfully.")
				}
			}
		}
	}

	for _, c := range containers {
		backupLogger().Debug("Finalize Restore: Start Container", "volume", destVol.Name, "container", c.ID)
		if !c.Start() {
			backupLogger().Warn("Failed to start container", "function", "Restore")
			projectEvent.PostEventUpdate("agent-a15b6d18583615a1", "Failed to start container")
		}
		time.Sleep(time.Second) // give each container a second to boot to avoid thrashing the disk
	}

}

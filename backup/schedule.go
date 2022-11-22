package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/cnslclient"
	"cs-agent/types"
	"os"
	"reflect"

	"github.com/getsentry/sentry-go"
	"github.com/google/uuid"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
)

func InitSchedule(c *cron.Cron) {
	backupLogger().Info("Starting backup scheduler")
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	consul, err := cnslclient.Client()
	if err != nil {
		panic(err)
	}
	// Clear out all existing schedules
	kv := consul.KV()
	schedulePath := "borg/nodes/" + hostname + "/schedules"
	_, _ = kv.DeleteTree(schedulePath, nil)
	c.Start()
	scheduleBackup(consul, c)
	_, err = c.AddFunc(viper.GetString("backups.check_freq"), func() { scheduleBackup(consul, c) })
	if err != nil {
		backupLogger().Warn("Fatal error scheduling backup.check_freq", "error", err.Error())
		sentry.CaptureException(err)
		return
	}
	_, err = c.AddFunc(viper.GetString("backups.prune_freq"), func() { prune(consul) })
	if err != nil {
		backupLogger().Warn("Fatal error scheduling backup.prune_freq", "error", err.Error())
		sentry.CaptureException(err)
		return
	}

}

func scheduleBackup(consul *consulAPI.Client, c *cron.Cron) {

	if reflect.ValueOf(consul).IsNil() {
		backupLogger().Warn("Consul client has gone away, skipping sequence")
		return
	}

	if reflect.ValueOf(c).IsNil() {
		backupLogger().Warn("Cron object has gone away, skipping sequence")
		return
	}

	hostname, _ := os.Hostname()
	kv := consul.KV()
	keys, _, err := kv.Keys("volumes", "", nil)
	if err != nil {
		backupLogger().Warn("Fatal error getting volume list from consul", "error", err.Error())
		sentry.CaptureException(err)
		return
	}

	for _, value := range keys {

		pair, _, err := kv.Get(value, nil)
		if err != nil {
			backupLogger().Warn("Fatal error getting volume key from consul", "volumePath", value, "error", err.Error())
			sentry.CaptureException(err)
			continue
		}

		if reflect.ValueOf(pair.Value).IsNil() {
			backupLogger().Info("Volume in consul has gone away, skipping", "volume", value)
			continue
		}

		vol, err := types.LoadVolume(pair.Value)

		if err != nil {
			backupLogger().Warn("Fatal error parsing volume from consul data", "volume", pair.Value, "error", err.Error())
			sentry.CaptureException(err)
			continue
		}

		repo := borg.Repository{Name: vol.Name}

		volJob, _ := vol.ScheduledJob(consul)

		if vol.Node == hostname {
			if vol.Trash {
				if volJob != nil {
					c.Remove(volJob.JID)
				}
				vol.ClearScheduledJob(consul)
				consulDeletePath(consul, "volumes/"+vol.Name)
				_, deleteRepoErr := repo.Delete()
				repo.StopContainer()
				if deleteRepoErr != nil {
					backupLogger().Warn("Error deleting repository", "volume", vol.Name, "response", deleteRepoErr.Error())
				}
			} else if vol.Backup {
				if volJob != nil && volJob.Schedule == vol.Freq {
					backupLogger().Debug("Backup schedule already exists for volume", "volume", vol.Name, "frequency", vol.Freq)
				} else {

					if volJob != nil {
						// job exists, but schedule must be different. Delete existing job and re-schedule
						backupLogger().Info("Rescheduling backup with new frequency", "volume", vol.Name, "newSchedule", vol.Freq, "oldSchedule", volJob.Schedule)
						c.Remove(volJob.JID)
						vol.ClearScheduledJob(consul)
					} else {
						backupLogger().Info("Configuring scheduled backup job", "volume", vol.Name, "schedule", vol.Freq)
					}

					jID, err := c.AddFunc(vol.Freq, func() { addBackupToQueue(vol) })

					if err != nil {
						backupLogger().Warn("Error creating scheduled backup", "volume", vol.Name, "error", err.Error())
						sentry.CaptureException(err)
						continue
					}
					addJidErr := vol.AddScheduledJob(consul, jID)
					if addJidErr != nil {
						backupLogger().Warn("Error writing jID to consul", "volume", vol.Name, "error", addJidErr.Error())
						sentry.CaptureException(err)
						continue
					}
				}

			} else if volJob != nil {
				backupLogger().Info("Deleting backup schedule for volume.", "volume", vol.Name, "frequency", vol.Freq)
				vol.ClearScheduledJob(consul)
			} else {
				backupLogger().Debug("Backups disabled for volume, skipping scheduling.", "volume", vol.Name)
			}
		} else if volJob != nil {
			backupLogger().Info("Volume no longer registered to me, removing schedule.", "volume", vol.Name, "frequency", vol.Freq)
			c.Remove(volJob.JID)
			vol.ClearScheduledJob(consul)
		} else {
			backupLogger().Debug("Skipping volume due to mismatched hostname", "volumeHostname", vol.Node, "hostname", hostname)
		}
	}
	if len(c.Entries()) < 1 {
		backupLogger().Debug("No backup jobs found.")
	}
}

func addBackupToQueue(vol types.Volume) {
	backupLogger().Info("Queueing automated backup for volume", "volume", vol.Name)
	consul, err := cnslclient.Client()
	if err != nil {
		backupLogger().Warn("Fatal error loading consul", "error", err.Error())
		sentry.CaptureException(err)
		return
	}
	newUUID := uuid.New()
	j := types.Job{
		ID:          "jobs/" + newUUID.String(),
		Name:        "volume.backup",
		VolumeName:  vol.Name,
		ArchiveName: "auto-{utcnow}",
		AuditID:     0,
		Node:        vol.Node,
	}
	err = j.Save(consul)
	if err != nil {
		backupLogger().Warn("Fatal error queue automated backup job", "error", err.Error(), "jid", "jobs/"+newUUID.String())
		sentry.CaptureException(err)
		return
	}
}

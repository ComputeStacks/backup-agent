package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/types"
	"os"
	"reflect"

	"github.com/getsentry/sentry-go"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
)

func InitSchedule(consul types.ConsulKV, c *cron.Cron) {
	backupLogger().Info("Starting backup scheduler")
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	// Clear out all existing schedules
	schedulePath := "borg/nodes/" + hostname + "/schedules"
	_, _ = consul.DeleteTree(schedulePath, nil)
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

	// Compaction moved in-agent (was a host cron on the backup server). Empty
	// disables scheduling; an invalid cron string is a loud failure.
	if compactFreq := viper.GetString("backups.compact_freq"); compactFreq != "" {
		_, err = c.AddFunc(compactFreq, func() { compact(consul) })
		if err != nil {
			backupLogger().Warn("Fatal error scheduling backups.compact_freq", "error", err.Error())
			sentry.CaptureException(err)
			return
		}
	} else {
		backupLogger().Info("Compaction scheduling disabled (backups.compact_freq is empty)")
	}

	// Periodic cleanup of stale export download records (completed past their
	// presigned-URL expiry, failed past retention). Inert unless export is
	// configured; an empty cleanup_freq disables it. Registered last so a bad
	// cron string can't short-circuit the registrations above.
	if viper.GetString("backups.export.s3.bucket") != "" {
		if cleanupFreq := viper.GetString("backups.export.cleanup_freq"); cleanupFreq != "" {
			_, err = c.AddFunc(cleanupFreq, func() { sweepExports(consul) })
			if err != nil {
				backupLogger().Warn("Fatal error scheduling backups.export.cleanup_freq", "error", err.Error())
				sentry.CaptureException(err)
				return
			}
		} else {
			backupLogger().Info("Export record cleanup disabled (backups.export.cleanup_freq is empty)")
		}
	}

}

func scheduleBackup(consul types.ConsulKV, c *cron.Cron) {

	if reflect.ValueOf(consul).IsNil() {
		backupLogger().Warn("Consul client has gone away, skipping sequence")
		return
	}

	if reflect.ValueOf(c).IsNil() {
		backupLogger().Warn("Cron object has gone away, skipping sequence")
		return
	}

	hostname, _ := os.Hostname()
	keys, _, err := consul.Keys("volumes", "", nil)
	if err != nil {
		backupLogger().Warn("Fatal error getting volume list from consul", "error", err.Error())
		return
	}

	for _, value := range keys {

		pair, _, err := consul.Get(value, nil)
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
				deleteVolumeExports(consul, vol.Name) // reap this volume's export download records
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

					jID, err := c.AddFunc(vol.Freq, func() { addBackupToQueue(consul, vol) })

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

func addBackupToQueue(consul types.ConsulKV, vol types.Volume) {
	backupLogger().Info("Queueing automated backup for volume", "volume", vol.Name)
	newUUID := uuid.New()
	j := types.Job{
		ID:          "jobs/" + newUUID.String(),
		Name:        "volume.backup",
		VolumeName:  vol.Name,
		ArchiveName: "auto-{utcnow}",
		AuditID:     0,
		Node:        vol.Node,
	}
	err := j.Save(consul)
	if err != nil {
		backupLogger().Warn("Fatal error queue automated backup job", "error", err.Error(), "jid", "jobs/"+newUUID.String())
		sentry.CaptureException(err)
		return
	}
}

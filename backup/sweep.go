package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/types"
	"encoding/json"
	"os"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/spf13/viper"
)

// sweepExports reaps stale export download records under borg/exports/ for
// volumes this node owns. It is the periodic counterpart to the boot-only
// ReconcileOrphanedExports and acts only on TERMINAL records:
//   - completed records whose presigned URL has expired, and
//   - failed records older than the retention window.
//
// It never touches "running" records — those are owned by the in-export safety
// net (finalizeStuckExport, for recovered panics) and ReconcileOrphanedExports
// at boot (for hard crashes). Because terminal records are never rewritten by
// anything else, deleting them races with nothing.
//
// All Consul I/O goes through the injected handle (not borg.ListDownloads /
// ConsulDownload.Save, which make their own client and would bypass the test
// mock), mirroring compact/prune.
func sweepExports(consul types.ConsulKV) {
	defer sentry.Recover()
	hostname, _ := os.Hostname()
	now := nowUnix()
	failedRetention := int64(viper.GetInt("backups.export.failed_retention_sec"))

	keys, _, err := consul.Keys("borg/exports/", "", nil)
	if err != nil {
		backupLogger().Warn("Export sweep: could not list download records", "error", err.Error())
		sentry.CaptureException(err)
		return
	}

	for _, key := range keys {
		volume := borg.DownloadVolumeFromKey(key)
		if volume == "" {
			continue // not a borg/exports/<volume>/<jid> record
		}
		pair, _, err := consul.Get(key, nil)
		if err != nil {
			backupLogger().Warn("Export sweep: could not read record", "key", key, "error", err.Error())
			continue
		}
		if pair == nil {
			continue // vanished mid-sweep
		}
		var d borg.ConsulDownload
		if json.Unmarshal(pair.Value, &d) != nil {
			continue // unparseable; leave it (could be a forward-compat schema)
		}
		// Only the node that owns the volume reaps its records. A deleted
		// volume's records are cleaned by the Trash path in scheduleBackup, not
		// here, to avoid cross-node guessing on a stale/missing volume read.
		if !ownsVolume(consul, volume, hostname) {
			continue
		}
		switch d.Status {
		case borg.DownloadStatusCompleted:
			if d.Expiry > 0 && d.Expiry < now {
				deleteKey(consul, key)
			}
		case borg.DownloadStatusFailed:
			if d.UpdatedAt > 0 && now-d.UpdatedAt > failedRetention {
				deleteKey(consul, key)
			}
		}
		// "running" records are intentionally never touched here — they are owned
		// by finalizeStuckExport (recovered panics) and ReconcileOrphanedExports
		// (hard crashes, at boot).
	}
}

// ownsVolume reports whether volumes/<volume> exists and is assigned to this
// node. The read is consistent so a stale follower read can't mislabel a live
// volume as gone. A missing volume, read error, or parse error all return false
// — the record is then skipped.
func ownsVolume(consul types.ConsulKV, volume, hostname string) bool {
	pair, _, err := consul.Get("volumes/"+volume, &consulAPI.QueryOptions{RequireConsistent: true})
	if err != nil || pair == nil {
		return false
	}
	vol, err := types.LoadVolume(pair.Value)
	if err != nil {
		return false
	}
	return vol.Node == hostname
}

func deleteKey(consul types.ConsulKV, key string) {
	if _, err := consul.Delete(key, nil); err != nil {
		backupLogger().Warn("Export sweep: failed to delete record", "key", key, "error", err.Error())
	}
}

// deleteVolumeExports removes all export download records for a volume. Called
// from scheduleBackup when a volume is trashed (the periodic sweep skips
// missing-volume records, so deletion time on the owning node is where they get
// cleaned). The trailing slash scopes the prefix to this exact volume so "vol1"
// can't also match "vol10".
func deleteVolumeExports(consul types.ConsulKV, volume string) {
	if _, err := consul.DeleteTree("borg/exports/"+volume+"/", nil); err != nil {
		backupLogger().Warn("Volume trash: failed to delete export records", "volume", volume, "error", err.Error())
	}
}

// putDownload writes d to key through the injected handle — a mock-friendly
// equivalent of ConsulDownload.Save (which makes its own client). Shared with
// finalizeStuckExport.
func putDownload(consul types.ConsulKV, key string, d *borg.ConsulDownload) {
	j, err := json.Marshal(d)
	if err != nil {
		backupLogger().Warn("Export record: marshal failed", "key", key, "error", err.Error())
		return
	}
	if _, err := consul.Put(&consulAPI.KVPair{Key: key, Value: j}, nil); err != nil {
		backupLogger().Warn("Export record: write failed", "key", key, "error", err.Error())
		sentry.CaptureException(err)
	}
}

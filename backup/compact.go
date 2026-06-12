package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/types"
	"hash/fnv"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/spf13/viper"
)

// compact reclaims space freed by prune/delete for every repository this node
// owns. It mirrors prune(): iterate volumes, filter to vol.Backup &&
// vol.Node == hostname, and run borg compact under the per-repo lock so it never
// overlaps an export of the same repo. Replaces the host-cron compactor.
func compact(consul types.ConsulKV) {
	defer sentry.Recover()
	hostname, _ := os.Hostname()

	// Per-node jitter so many nodes sharing one backup server don't all start
	// compacting at the same cron minute. Deterministic (hostname-derived) for
	// even spread and reproducibility. Each cron job runs in its own goroutine,
	// so sleeping here doesn't delay other scheduled jobs.
	if jitter := viper.GetInt("backups.compact_jitter_sec"); jitter > 0 {
		time.Sleep(jitterDelay(hostname, jitter))
	}

	keys, _, err := consul.Keys("volumes", "", nil)
	if err != nil {
		backupLogger().Warn("Compact error getting volume list from consul", "error", err.Error())
		sentry.CaptureException(err)
		return
	}

	for _, value := range keys {
		pair, _, err := consul.Get(value, nil)
		if err != nil {
			backupLogger().Warn("Compact error getting volume from consul", "volumePath", value, "error", err.Error())
			sentry.CaptureException(err)
			continue
		}
		vol, err := types.LoadVolume(pair.Value)
		if err != nil {
			backupLogger().Warn("Fatal error parsing volume from consul data", "volume", pair.Value, "error", err.Error())
			sentry.CaptureException(err)
			continue
		}
		if vol.Backup && vol.Node == hostname {
			// Scoped closure so the lock releases each iteration (and on panic),
			// and so one repo blocked behind an in-flight export doesn't stall
			// the rest of the sweep.
			func() {
				defer borg.AcquireRepoLock(vol.Name)()
				repo := borg.Repository{Name: vol.Name, SourceVolumeName: vol.Name}
				if log := repo.Compact(); log != nil {
					backupLogger().Warn("Compact Volume Error", "volume", vol.Name, "error", log.Message)
				}
				repo.StopContainer() // no-op for the NFS backend (no container)
			}()
		}
	}
}

// jitterDelay maps a hostname to a stable delay in [0, maxSec).
func jitterDelay(hostname string, maxSec int) time.Duration {
	if maxSec <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(hostname))
	return time.Duration(int(h.Sum32()%uint32(maxSec))) * time.Second
}

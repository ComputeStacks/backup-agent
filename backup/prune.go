package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/types"
	"os"

	"github.com/getsentry/sentry-go"
)

func prune(consul types.ConsulKV) {
	defer sentry.Recover()
	hostname, _ := os.Hostname()
	keys, _, err := consul.Keys("volumes", "", nil)
	if err != nil {
		panic(err)
	}

	for _, value := range keys {

		pair, _, err := consul.Get(value, nil)
		if err != nil {
			panic(err)
		}
		vol, err := types.LoadVolume(pair.Value)
		if err != nil {
			backupLogger().Warn("Fatal error parsing volume from consul data", "volume", pair.Value, "error", err.Error())
			sentry.CaptureException(err)
			continue
		}
		if vol.Backup && vol.Node == hostname {
			// Serialize against compact/export of the same repo. Scoped to a
			// closure so the lock releases each iteration (and on panic), not at
			// function return.
			func() {
				defer borg.AcquireRepoLock(vol.Name)()
				repo, repoErr := borg.FindRepository(&vol, &vol)
				if repoErr != nil {
					backupLogger().Warn("Prune Volume Error, error loading repo", "volume", vol.Name, "error", repoErr.Message)
					return
				}
				if err := repo.Prune(); err != nil {
					backupLogger().Warn("Prune Volume Error", "volume", vol.Name)
				}
				repo.Container.Stop()
			}()
		}
	}

}

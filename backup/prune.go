package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/types"
	"os"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
)

func prune(consul *consulAPI.Client) {
	defer sentry.Recover()
	hostname, _ := os.Hostname()
	kv := consul.KV()
	keys, _, err := kv.Keys("volumes", "", nil)
	if err != nil {
		panic(err)
	}

	for _, value := range keys {

		pair, _, err := kv.Get(value, nil)
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
			repo, repoErr := borg.FindRepository(&vol, &vol)

			if repoErr != nil {
				backupLogger().Warn("Prune Volume Error, error loading repo", "volume", vol.Name, "error", repoErr.Message)
			} else {
				err := repo.Prune()
				if err != nil {
					backupLogger().Warn("Prune Volume Error", "volume", vol.Name)
				}
				repo.Container.Stop()
			}
		}
	}

}

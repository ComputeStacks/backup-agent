package job

import (
	"context"
	"cs-agent/backup"
	"cs-agent/cnslclient"
	"cs-agent/firewall"
	"cs-agent/log"
	"cs-agent/types"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/viper"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
)

// Watch for new jobs
func Watch(wg *sync.WaitGroup) {
	defer sentry.Recover()
	defer wg.Done()
	defer func() {
		jobEvent().Info("Watch worker stopping")
	}()
	consul, err := cnslclient.Client()
	if err != nil {
		jobEvent().Warn("Fatal error loading consul", "error", err.Error())
		sentry.CaptureException(err)
		return
	}
	hostname, _ := os.Hostname()

	// On boot, trigger firewall reload
	go firewall.Perform(consul)

	// Build Job Queues
	ctx, cancel := context.WithCancel(context.Background())
	backupWorkerCount := viper.GetInt("queue.numworkers") + 1
	exportWorkerCount := viper.GetInt("backups.export.workers")
	if exportWorkerCount < 1 {
		exportWorkerCount = 1
	}
	backupQueue := make(chan types.Job)
	ipTableQueue := make(chan types.Job)
	exportQueue := make(chan types.Job)
	setupWorkers(ctx, wg, consul, "backup", backupWorkerCount, backupQueue)
	setupWorkers(ctx, wg, consul, "firewall", 1, ipTableQueue)
	setupWorkers(ctx, wg, consul, "export", exportWorkerCount, exportQueue)
	captureExit(cancel)

	// On boot, before dispatching, reconcile exports left "running" by a crashed
	// process (mark failed + drop the orphaned job so it isn't auto-re-run).
	// Synchronous so it can't race a re-dispatch of the same job.
	if viper.GetString("backups.export.s3.bucket") != "" {
		backup.ReconcileOrphanedExports(consul)
	}

	kvClient := consul.KV()
	opts := &consulAPI.QueryOptions{AllowStale: false}
	errCount := 0

	for {
		events, meta, err := kvClient.Keys("jobs", "", opts)
		if err != nil {
			jobEvent().Warn("Fatal error loading job list", "error", err.Error())
			if errCount > 12 {
				jobEvent().Warn("Error count has reach more than 12, stopping all jobs")
				return
			}
			errCount = errCount + 1
			time.Sleep(5 * time.Second) // Wait 5 seconds
			continue
		}
		errCount = 0
		for _, value := range events {
			data, _, err := kvClient.Get(value, nil)
			if err != nil {
				jobEvent().Warn("Fatal error loading job", "error", err.Error())
				sentry.CaptureException(err)
				continue
			}
			var job types.Job
			if data == nil {
				continue // We can get here if the data was deleted.
			}
			err = json.Unmarshal(data.Value, &job)
			if err != nil {
				jobEvent().Warn("Fatal error loading job", "jobID", data.Key, "error", err.Error())
				sentry.CaptureException(err)
				continue
			}
			job.ID = data.Key
			if hostname == job.Node {
				switch job.Name {
				case "firewall":
					ipTableQueue <- job
					job.Close(consul.KV())
				case "backup.export":
					// Long-running: hand off without blocking the single dispatch
					// goroutine, and do NOT delete the KV key here — ExportBackup
					// deletes it on completion (processJob). Dedup so the
					// still-present key isn't re-dispatched while in flight.
					if markExportInFlight(job.ID) {
						select {
						case exportQueue <- job:
						default:
							// export worker busy: leave the KV key in place; it's
							// re-dispatched on the next jobs/ change (e.g. when the
							// busy export finishes and deletes its key)
							clearExportInFlight(job.ID)
						}
					}
				default:
					backupQueue <- job
					job.Close(consul.KV())
				}
			}

		}
		opts.WaitIndex = meta.LastIndex
	}
	// https://github.com/hashicorp/consul/blob/master/api/lock.go#L357-L384
}

func processJob(consul *consulAPI.Client, job *types.Job) {
	defer sentry.Recover()
	jobEvent().Info("Processing job", "job", job.ID, "kind", job.Name)
	switch job.Name {
	case "volume.backup":
		resolveArchiveName(job)
		_ = backup.Perform(consul, job)
	case "volume.restore":
		backup.Restore(consul, job)
	case "backup.delete":
		_ = backup.DeleteBackup(consul, job)
	case "backup.export":
		// The KV key and in-flight marker are cleared only after the export
		// finishes. defer LIFO: Close (delete key) runs before clearing the
		// marker, so the deleted key can't be re-dispatched in the gap.
		func() {
			defer clearExportInFlight(job.ID)
			defer job.Close(consul.KV())
			backup.ExportBackup(consul, job)
		}()
	case "firewall":
		firewall.Perform(consul)
	default:
		jobEvent().Info("Unknown job", "name", job.Name)
	}

}

func jobEvent() hclog.Logger {
	return log.New().Named("worker")
}

// captureExit cancels the worker context on SIGINT/SIGTERM so the worker pools
// drain their in-flight job and return (each calls wg.Done). It deliberately
// does NOT call os.Exit: process teardown is owned by main, which on the same
// signal drains the metadata HTTP server and closes the store in order. Calling
// os.Exit here would bypass main's deferred store Close and abort an in-flight
// HTTP drain mid-flight (it ran the defers-skipping os.Exit race this replaces).
func captureExit(cancel context.CancelFunc) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Stopping workers...")
		cancel()
	}()
}

func resolveArchiveName(job *types.Job) {
	switch job.ArchiveName {
	case "":
		job.ArchiveName = "manual-m-{utcnow}"
	case "auto":
		job.ArchiveName = "auto-{utcnow}"
	default:
		job.ArchiveName = job.ArchiveName + "-m-{utcnow}"
	}
}

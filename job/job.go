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
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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

	// On boot, trigger iptable reload
	log.New().Info("Syncing firewall")
	go firewall.Perform(consul)

	// Build Job Queues
	ctx, cancel := context.WithCancel(context.Background())
	backupWorkerCount := viper.GetInt("queue.numworkers") + 1
	backupQueue := make(chan types.Job)
	iptableQueue := make(chan types.Job)
	setupWorkers(ctx, wg, consul, "backup", backupWorkerCount, backupQueue)
	setupWorkers(ctx, wg, consul, "firewall", 1, iptableQueue)
	captureExit(cancel)

	kvClient := consul.KV()
	opts := &consulAPI.QueryOptions{AllowStale: false}
	errCount := 0
WAIT:
	events, meta, err := kvClient.Keys("jobs", "", opts)
	if err != nil {
		jobEvent().Warn("Fatal error loading job list", "error", err.Error())
		if errCount > 12 {
			jobEvent().Warn("Error count has reach more than 12, stopping all jobs")
			return
		}
		errCount = errCount + 1
		time.Sleep(5 * time.Second) // Wait 5 seconds
		goto WAIT
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
			if job.Name == "firewall" {
				iptableQueue <- job
			} else {
				backupQueue <- job
			}
			job.Close(consul)
		}

	}
	opts.WaitIndex = meta.LastIndex
	goto WAIT
	// https://github.com/hashicorp/consul/blob/master/api/lock.go#L357-L384
}

func processJob(consul *consulAPI.Client, job *types.Job) {
	defer sentry.Recover()
	jobEvent().Info("Processing job", "job", job.ID, "kind", job.Name)
	switch job.Name {
	case "volume.backup":
		if job.ArchiveName == "" {
			job.ArchiveName = "manual-m-{utcnow}"
		} else if job.ArchiveName == "auto" {
			job.ArchiveName = "auto-{utcnow}"
		} else {
			job.ArchiveName = job.ArchiveName + "-m-{utcnow}"
		}
		_ = backup.Perform(consul, job)
	case "volume.restore":
		backup.Restore(consul, job)
	case "backup.delete":
		_ = backup.DeleteBackup(consul, job)
	case "firewall":
		firewall.Perform(consul)
	default:
		jobEvent().Info("Unknown job", "name", job.Name)
	}
	return
}

func jobEvent() hclog.Logger {
	return log.New().Named("worker")
}

func captureExit(cancel context.CancelFunc) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Stopping workers...")
		cancel()
		os.Exit(0)
	}()
}

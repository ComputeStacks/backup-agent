package main

import (
	"cs-agent/backup"
	"cs-agent/cnslclient"
	"cs-agent/config"
	"cs-agent/job"
	"cs-agent/log"
	"cs-agent/s3upload"
	"os"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
)

func main() {
	var wg sync.WaitGroup
	v := "1.10.0"
	config.ConfigureApp()
	configureSentry(v)
	validateExportConfig()
	ensureConsulReady()
	wg.Add(1) // job.Watch(); setupWorkers() registers each worker pool's own count
	if viper.GetBool("backups.enabled") {
		c := cron.New()
		consul, err := cnslclient.Client()
		if err != nil {
			panic(err)
		}
		backup.InitSchedule(consul.KV(), c)
	}
	go job.Watch(&wg)
	log.New().Info("Starting CS-Agent", "version", v)
	log.New().Info("Agent Configuration", "environment", config.ReleaseEnvironment())
	log.New().Info("Agent Configuration", "backupWorkers", viper.GetInt("queue.numworkers")+1)
	log.New().Info("Agent Configuration", "firewallWorkers", 1)
	log.New().Info("Agent Configuration", "backingFS", currentBackupMethod())

	//select {} // Hold open the process
	wg.Wait()
}

func currentBackupMethod() string {
	if viper.GetBool("backups.borg.ssh.enabled") {
		return "ssh"
	} else if viper.GetBool("backups.borg.nfs") {
		return "nfs"
	} else {
		return "local"
	}
}

// validateExportConfig loudly surfaces export misconfiguration at startup. It
// does not abort the agent (export jobs fail individually if misconfigured), but
// the no-compactor combination is dangerous enough to flag prominently: with
// export enabled and the host compact cron retired, an empty compact_freq means
// nothing ever compacts repos and they grow unbounded.
func validateExportConfig() {
	if viper.GetString("backups.export.s3.bucket") == "" {
		return // export disabled (no bucket)
	}
	if viper.GetString("backups.compact_freq") == "" {
		log.New().Error("backups.export is enabled but backups.compact_freq is empty: with the host compact cron retired, nothing compacts repositories and they will grow unbounded. Set backups.compact_freq.")
		sentry.CaptureMessage("backup export enabled with compaction disabled (no compactor)")
	}
	if err := s3upload.ConfigFromViper().Validate(0); err != nil {
		log.New().Error("Invalid backups.export.s3 configuration", "error", err.Error())
		sentry.CaptureException(err)
	}
}

func ensureConsulReady() {
	count := 0
RETRY:
	cli, err := cnslclient.Client()
	if err != nil {
		log.New().Error("Error loading consul config", "error", err.Error())
		panic(err)
	}

	statusObj := cli.Status()
	_, err = statusObj.Leader()

	if err != nil {
		if count > 10 {
			log.New().Error("Fatal error, unable to connect to consul")
			panic(err)
		}
		count = count + 1
		log.New().Warn("Failed to connect to consul", "retry", count)
		time.Sleep(5 * time.Second)
		goto RETRY
	}

}

/*
*
Configure Sentry

Resources:
  - https://github.com/getsentry/sentry-go
  - https://github.com/getsentry/sentry-go/blob/master/MIGRATION.md
  - https://docs.sentry.io/clients/go/
*/
func configureSentry(v string) {
	env := config.ReleaseEnvironment()
	hostname, _ := os.Hostname()
	err := sentry.Init(sentry.ClientOptions{
		Dsn:              viper.GetString("sentry.dsn"),
		Environment:      env,
		Debug:            env != "production",
		ServerName:       hostname,
		AttachStacktrace: true,
		Release:          v,
	})
	if err != nil {
		panic(err)
	}
}

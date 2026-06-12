package main

import (
	"cs-agent/backup"
	"cs-agent/cnslclient"
	"cs-agent/config"
	"cs-agent/job"
	"cs-agent/log"
	"os"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
)

func main() {
	var wg sync.WaitGroup
	v := "1.9.0"
	config.ConfigureApp()
	configureSentry(v)
	ensureConsulReady()
	wgCount := 1 + viper.GetInt("queue.numworkers") // job.Watch() + Workers that will be created
	wg.Add(wgCount)
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
	log.New().Info("Agent Configuration", "backupWorkers", wgCount)
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

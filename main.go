/**

TODO: Reporting
	- Report SystemEvents on errors

	CS Event Log Codes: `echo agent-$(openssl rand -hex 8) | tr -d '\n' | pbcopy`

TODO: Store work state in consul
	- Ensure that backup and restore jobs don't take place while another job is actively
      running. Create a queue in consul called `borg/work/<volume-id>`. Only allow 1 job at a time.
    - The schema for the work queue could be something like:
		```yaml
		# /borg/work/abcdef-adfgwef-...-efsdf
		job: backup.create
		jid: jobID # For queued up work, use this to determine who's turn it is to work!
		started: Time-as-int
		queue: []job # or not? just leave the original job alone until this is gone.
		```
		once the work is done, begin to work through the queue.

TODO: Determine proper way of cleaning up jobs and volume data in consul if they no longer exist on the node.

TODO: Handle worker processes that die and kill the entire app.

*/

package main

import (
	"cs-agent/backup"
	"cs-agent/cnslclient"
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
	v := "1.3.6"
	configureApp()
	configureSentry(v)
	ensureConsulReady()
	wgCount := 1 + viper.GetInt("queue.numworkers") // job.Watch() + Workers that will be created
	wg.Add(wgCount)
	if viper.GetBool("backups.enabled") {
		c := cron.New()
		backup.InitSchedule(c)
	}
	go job.Watch(&wg)
	log.New().Info("Starting CS-Agent", "version", v, "environment", ReleaseEnvironment(), "backupWorkers", wgCount, "firewallWorkers", 1)

	//select {} // Hold open the process
	wg.Wait()
}

func configureApp() {
	viper.SetConfigType("yaml")

	// Load File
	viper.SetConfigName("agent")              // name of config file (without extension)
	viper.AddConfigPath("/etc/computestacks") // path to look for the config file in
	viper.AddConfigPath(".")                  // optionally look for config in the working directory
	err := viper.ReadInConfig()               // Find and read the config file
	if err != nil {                           // Handle errors reading the config file
		log.New().Warn("Error loading configuration file", "error", err)
	}

	////
	// Defaults
	viper.SetDefault("log.level", "INFO")
	viper.SetDefault("sentry.dsn", "https://caf0e228c0dc4c36a4b4972cc2c0eba2@sentry.cmptstks.com/3")

	// For testing purposes only, dont set `true` in production environments.
	viper.SetDefault("docker.privileged", false)

	viper.SetDefault("docker.version", "1.41")

	viper.SetDefault("computestacks.host", "localhost:3000")

	viper.SetDefault("queue.numworkers", 3)

	viper.SetDefault("consul.host", "127.0.0.1:8500")
	viper.SetDefault("consul.token", "")
	viper.SetDefault("consul.tls", false)

	viper.SetDefault("backups.enabled", true)
	viper.SetDefault("backups.check_freq", "* * * * *")
	viper.SetDefault("backups.prune_freq", "15 1 * * *")
	viper.SetDefault("backups.key", "changeme!")

	viper.SetDefault("backups.borg.image", "ghcr.io/computestacks/cs-docker-borg:latest")
	viper.SetDefault("backups.borg.compression", "zstd,3")

	viper.SetDefault("backups.borg.nfs", false)
	viper.SetDefault("backups.borg.nfs_host", "127.0.0.1")
	viper.SetDefault("backups.borg.nfs_opts", ",async,noatime,rsize=32768,wsize=32768")

	viper.SetDefault("backups.borg.nfs_host_path", "/var/nfsshare/volume_backups")
	viper.SetDefault("backups.borg.nfs_ssh.user", "root")
	viper.SetDefault("backups.borg.nfs_ssh.port", "22")
	viper.SetDefault("backups.borg.nfs_ssh.keyfile", "/root/.ssh/id_ed25519")

	// Whether we ssh into the backup server and create the path.
	viper.SetDefault("backups.borg.nfs_create_path", true)

	viper.SetDefault("backups.borg.fs.user", "nfsnobody")
	viper.SetDefault("backups.borg.fs.group", "nfsnobody")

	// MariaDB Backup Configuration
	viper.SetDefault("mariadb.lock_wait.query_type", "ALL")
	viper.SetDefault("mariadb.lock_wait.timeout", "60")
	viper.SetDefault("mariadb.long_queries.timeout", "20")
	viper.SetDefault("mariadb.long_queries.query_type", "SELECT")

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
	env := ReleaseEnvironment()
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

// ReleaseEnvironment is a helper used to determine current release
func ReleaseEnvironment() string {
	if viper.GetString("backups.key") == "changeme!" {
		return "development"
	} else if viper.GetString("backups.key") == "tester!" {
		return "testing"
	} else {
		return "production"
	}
}

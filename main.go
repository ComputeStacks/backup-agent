package main

import (
	"context"
	"cs-agent/backup"
	"cs-agent/cnslclient"
	"cs-agent/config"
	"cs-agent/httpapi"
	"cs-agent/job"
	"cs-agent/log"
	"cs-agent/s3upload"
	"cs-agent/store"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
)

// Build-info, injected at release time via GoReleaser ldflags
// (-X main.version / -X main.commit / -X main.date). Defaults apply to
// `go build` / `go run` so behavior is unchanged when unset.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	var wg sync.WaitGroup

	showVersion := flag.Bool("version", false, "print version information and exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("cs-agent %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	config.ConfigureApp()
	configureSentry(version)
	log.New().Info("Starting CS-Agent", "version", version, "commit", commit, "date", date)
	validateExportConfig()

	// Bring up the embedded data plane + the customer-metadata HTTP front door
	// BEFORE the Consul gate. Metadata serving does not depend on Consul (auth is
	// the local control.db, data is per-project SQLite), so a slow/absent Consul
	// must not delay or block it — customer containers reach metadata.internal:8500
	// regardless of cluster health.
	metaStore, metaServer := startMetadataServer()

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
	log.New().Info("Agent Configuration", "environment", config.ReleaseEnvironment())
	log.New().Info("Agent Configuration", "backupWorkers", viper.GetInt("queue.numworkers")+1)
	log.New().Info("Agent Configuration", "firewallWorkers", 1)
	log.New().Info("Agent Configuration", "backingFS", currentBackupMethod())

	// Single coordinated shutdown. On SIGTERM/SIGINT the worker context is also
	// cancelled inside job.Watch (captureExit), so the worker pools drain their
	// in-flight job and call wg.Done(); here we own the ORDERED process teardown.
	//
	// Sequence (HTTP must drain BEFORE the store closes — in-flight customer
	// requests use the store):
	//  1. wait (bounded) for the worker pools to stop, so a backup mid-write
	//     finishes rather than being killed;
	//  2. drain the metadata HTTP server (finish in-flight customer requests);
	//  3. close the store.
	//
	// The wait is BOUNDED: job.Watch's own dispatch loop blocks on a Consul
	// blocking query and never observes the cancel, so wg never fully drains.
	// We therefore wait only long enough for the worker pools to wind down a
	// real job, then proceed regardless — the agent still exits promptly on
	// SIGTERM (a hung/long borg run can't hold shutdown open forever).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.New().Info("Shutdown signal received, draining")

	waitWorkers(&wg, 25*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if metaServer != nil {
		if err := metaServer.Shutdown(ctx); err != nil {
			log.New().Warn("metadata server shutdown", "error", err)
		}
	}
	if metaStore != nil {
		if err := metaStore.Close(); err != nil {
			log.New().Warn("metadata store close", "error", err)
		}
	}
	log.New().Info("Shutdown complete")
}

// waitWorkers blocks until wg drains or timeout elapses, whichever is first.
// Bounded so shutdown can't hang forever on the job.Watch dispatch goroutine
// (which blocks on a Consul query and never observes the worker cancel) or on a
// worker stuck in a long-running job; on timeout it logs and returns so the
// ordered teardown (HTTP drain → store close) still runs and the process exits
// promptly.
func waitWorkers(wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.New().Info("Workers stopped")
	case <-time.After(timeout):
		log.New().Warn("Workers did not stop within timeout; proceeding with shutdown", "timeout", timeout.String())
	}
}

// startMetadataServer opens the embedded data plane (store/) and starts the
// customer-metadata HTTP front door in a goroutine. It is intentionally
// independent of ensureConsulReady() — metadata serving needs no Consul. A
// failure to open the store is fatal (the agent's whole reason to bind :8500 is
// gone); a clean Shutdown closes the listener and ListenAndServe returns
// http.ErrServerClosed, which is not logged as an error.
func startMetadataServer() (*store.Store, *httpapi.Server) {
	st, err := store.Open(viper.GetString("store.data_dir"), store.Options{})
	if err != nil {
		log.New().Error("Failed to open metadata store", "error", err.Error())
		sentry.CaptureException(err)
		panic(err)
	}

	srv := httpapi.New(httpapi.Config{
		ListenAddr:     viper.GetString("metadata.listen_addr"),
		AdminTokenHash: viper.GetString("metadata.admin_token_hash"),
		MaxBodyBytes:   viper.GetInt64("metadata.max_body_bytes"),
		ProxyToConsul:  viper.GetBool("metadata.proxy_to_consul"),
	}, st, log.New())

	go func() {
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.New().Error("metadata HTTP server stopped", "error", err.Error())
			sentry.CaptureException(err)
		}
	}()

	return st, srv
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

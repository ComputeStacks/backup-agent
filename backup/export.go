package backup

import (
	"context"
	"crypto/rand"
	"cs-agent/backup/borg"
	"cs-agent/csevent"
	"cs-agent/s3upload"
	"cs-agent/types"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/spf13/viper"
)

// Event codes for the backup-export operation.
//
// PLACEHOLDER: exportEventCode is the operation code ComputeStacks must
// pre-create the EventLog with (it matches the agent's status callback by
// audit_id + this code). The ComputeStacks side MUST use the same value — see
// the handoff doc in the computestacks repo. exportDetailFailed labels the
// failure-detail PATCH and is agent-internal.
const (
	exportEventCode     = "agent-e7c1a9d4b6f20835"
	exportDetailFailed  = "agent-3f8b2c1e7a9d4056"
	exportEventLocale   = "volume.download"
	exportArchiveSuffix = ".tar"
)

var objectKeyUnsafe = regexp.MustCompile(`[^a-zA-Z0-9._-]`)

// ExportBackup streams a chosen archive to S3 and publishes a presigned GET URL
// to borg/exports/<volume>/<jid> for ComputeStacks to read. The EventLog status
// (running -> completed/failed) is the readiness gate; the URL travels via KV.
//
// Runs under the per-repo lock so a compact never rewrites segments mid-stream.
// The borg export uses --bypass-lock, so scheduled backups (create) are never
// blocked. A presigned URL is published ONLY when both the borg export exited 0
// and the upload succeeded.
func ExportBackup(consul *consulAPI.Client, job *types.Job) {
	defer sentry.Recover()
	hostname, _ := os.Hostname()
	kv := consul.KV()
	opts := &consulAPI.QueryOptions{RequireConsistent: true}
	jid := strings.TrimPrefix(job.ID, "jobs/")

	data, _, err := kv.Get("volumes/"+job.VolumeName, opts)
	if err != nil {
		backupLogger().Warn("Export: error loading volume from consul", "volume", job.VolumeName, "error", err.Error())
		sentry.CaptureException(err)
		return
	}
	if data == nil {
		backupLogger().Warn("Export: skipping unknown volume", "volume", job.VolumeName)
		saveDownload(borg.DownloadKey(job.VolumeName, jid), &borg.ConsulDownload{
			Status: borg.DownloadStatusFailed, Error: "unknown volume", UpdatedAt: nowUnix(),
		})
		return
	}
	vol, err := types.LoadVolume(data.Value)
	if err != nil {
		backupLogger().Warn("Export: error parsing volume", "volume", job.VolumeName, "error", err.Error())
		sentry.CaptureException(err)
		return
	}
	if vol.Node != hostname {
		backupLogger().Info("Export: skipping volume not under my control", "volume", job.VolumeName)
		saveDownload(borg.DownloadKey(vol.Name, jid), &borg.ConsulDownload{
			Status: borg.DownloadStatusFailed, Error: "volume is not assigned to this node", UpdatedAt: nowUnix(),
		})
		return
	}

	dlKey := borg.DownloadKey(vol.Name, jid)

	projectEvent := csevent.New(vol.ProjectID, []int{vol.ID}, exportEventCode, exportEventLocale, job.AuditID)
	defer func() {
		if projectEvent != nil {
			projectEvent.CloseEvent() // promotes a still-"running" event to "completed"
		}
	}()

	saveDownload(dlKey, &borg.ConsulDownload{Status: borg.DownloadStatusRunning, UpdatedAt: nowUnix()})

	// S3 destination (credentials are node-local config, never from the job).
	s3cfg := s3upload.ConfigFromViper()
	if !s3cfg.Enabled() {
		failExport(projectEvent, dlKey, "backup export is not configured (no S3 bucket)")
		return
	}
	if cfgErr := s3cfg.Validate(0); cfgErr != nil {
		failExport(projectEvent, dlKey, cfgErr.Error())
		return
	}
	uploader, err := s3upload.New(s3cfg)
	if err != nil {
		failExport(projectEvent, dlKey, "s3 init: "+err.Error())
		return
	}

	// Bound the whole export so a hung borg or a stalled S3 endpoint can't hold
	// the per-repo lock or the export worker forever.
	ctx := context.Background()
	if t := viper.GetInt("backups.export.timeout_sec"); t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(t)*time.Second)
		defer cancel()
	}

	// Serialize against compact/prune of the same repo for the whole stream.
	defer borg.AcquireRepoLock(vol.Name)()

	repo, findErr := borg.FindRepository(&vol, &vol)
	if findErr != nil {
		failExport(projectEvent, dlKey, "find repository: "+findErr.Message)
		return
	}
	archive, archErr := repo.FindArchive(job.ArchiveName)
	if archErr != nil {
		repo.StopContainer()
		failExport(projectEvent, dlKey, "find archive: "+archErr.Message)
		return
	}

	objectKey := jid + "/" + randomToken() + "/" +
		sanitizeKeySegment(vol.Name) + "-" + sanitizeKeySegment(job.ArchiveName) + exportArchiveSuffix

	// Stream: borg export-tar (producer) -> io.Pipe -> S3 multipart upload.
	pr, pw := io.Pipe()
	exportErrCh := make(chan *borg.LogMessage, 1)
	go func() {
		lg := archive.ExportTar(ctx, pw)
		if lg != nil {
			// Make the uploader's Read fail so it can't Complete a truncated tar.
			pw.CloseWithError(errors.New(lg.Message))
		} else {
			pw.Close()
		}
		exportErrCh <- lg
	}()

	size, upErr := uploader.Upload(ctx, objectKey, pr)

	// If the upload abandoned the read (error or timeout), unblock the producer's
	// pw.Write so the export goroutine can't leak.
	pr.CloseWithError(upErr)

	// Stream is drained; safe to tear the container down now (not via an early defer).
	repo.StopContainer()

	exportLog := <-exportErrCh // synchronizes with the producer goroutine

	// Publish a URL ONLY if borg exited 0 AND the upload succeeded.
	if exportLog != nil {
		failExport(projectEvent, dlKey, "export collided with a concurrent repo write or borg failed (retry): "+exportLog.Message)
		return
	}
	if upErr != nil {
		failExport(projectEvent, dlKey, "upload failed: "+upErr.Error())
		return
	}

	url, expiry, psErr := uploader.PresignGet(ctx, objectKey, time.Duration(job.DownloadTTL)*time.Second)
	if psErr != nil {
		failExport(projectEvent, dlKey, "presign failed: "+psErr.Error())
		return
	}

	saveDownload(dlKey, &borg.ConsulDownload{
		Status:    borg.DownloadStatusCompleted,
		URL:       url,
		ObjectKey: s3cfg.Prefix + objectKey,
		Size:      size,
		Expiry:    expiry.Unix(),
		UpdatedAt: nowUnix(),
	})
	backupLogger().Info("Completed backup export", "volume", vol.Name, "archive", job.ArchiveName, "size", size)
	// projectEvent is promoted running -> completed by the deferred CloseEvent.
}

// ReconcileOrphanedExports marks any export record still "running" for a volume
// this node owns as "failed". On boot no export is in flight yet, so a lingering
// "running" record is necessarily orphaned by a crash/restart mid-stream. The
// owner check avoids clobbering an export legitimately running on another node.
func ReconcileOrphanedExports(consul *consulAPI.Client) {
	defer sentry.Recover()
	hostname, _ := os.Hostname()
	downloads, err := borg.ListDownloads()
	if err != nil {
		backupLogger().Warn("Export reconcile: could not list exports", "error", err.Error())
		return
	}
	for key, d := range downloads {
		if d.Status != borg.DownloadStatusRunning {
			continue
		}
		volume := borg.DownloadVolumeFromKey(key)
		if volume == "" {
			continue
		}
		vdata, _, vErr := consul.KV().Get("volumes/"+volume, nil)
		if vErr != nil || vdata == nil {
			continue
		}
		v, vlErr := types.LoadVolume(vdata.Value)
		if vlErr != nil || v.Node != hostname {
			continue // not ours; leave it alone
		}
		backupLogger().Warn("Export reconcile: marking orphaned export failed", "key", key)
		d.Status = borg.DownloadStatusFailed
		d.Error = "agent restarted during export; please retry"
		d.UpdatedAt = nowUnix()
		if saveErr := d.Save(key); saveErr != nil {
			backupLogger().Warn("Export reconcile: failed to update record", "key", key, "error", saveErr.Error())
		}
		// Drop the orphaned job so it isn't auto-re-run (no blind 50GB resume);
		// the user re-requests if they still want the download.
		if oj := borg.DownloadJidFromKey(key); oj != "" {
			if _, delErr := consul.KV().Delete("jobs/"+oj, nil); delErr != nil {
				backupLogger().Warn("Export reconcile: failed to delete orphaned job", "jid", oj, "error", delErr.Error())
			}
		}
	}
}

func failExport(event *csevent.ProjectEvent, dlKey, msg string) {
	backupLogger().Warn("Backup export failed", "error", msg)
	saveDownload(dlKey, &borg.ConsulDownload{Status: borg.DownloadStatusFailed, Error: msg, UpdatedAt: nowUnix()})
	if event != nil {
		event.EventLog.Status = "failed"
		event.PostEventUpdate(exportDetailFailed, msg)
	}
}

func saveDownload(key string, d *borg.ConsulDownload) {
	if err := d.Save(key); err != nil {
		backupLogger().Warn("Export: failed to write download record", "key", key, "error", err.Error())
		sentry.CaptureException(err)
	}
}

func nowUnix() int64 { return time.Now().Unix() }

func randomToken() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is effectively impossible; fall back to a timestamp.
		return hex.EncodeToString([]byte(time.Now().Format("150405.000000")))
	}
	return hex.EncodeToString(b)
}

func sanitizeKeySegment(s string) string {
	return objectKeyUnsafe.ReplaceAllString(s, "_")
}

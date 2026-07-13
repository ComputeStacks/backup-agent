package backup

import (
	"context"
	"cs-agent/store"
	"encoding/json"
	"errors"
	"fmt"
)

// taskParams carries the task fields that don't have a dedicated store.Task
// column — the old types.Job.SourceVolumeName/FilePaths/DownloadTTL — as opaque
// JSON in task.Params.
type taskParams struct {
	SourceVolume string   `json:"source_volume"`
	FilePaths    []string `json:"file_paths"`
	DownloadTTL  int      `json:"download_ttl"`
}

func parseParams(task store.Task) taskParams {
	var p taskParams
	if len(task.Params) > 0 {
		_ = json.Unmarshal(task.Params, &p)
	}
	return p
}

// RunTask executes a task and returns its result_json plus an error. It is the
// single entry point the dispatcher's worker calls; the worker records the
// outcome via store.UpdateTaskStatus. On any failure — a returned error OR a soft
// failure a handler recorded via progress.EventLog.Status — the result carries the
// accumulated failure output (per the v3.0.0 "terminal result + failure output"
// contract; there is no live per-step stream anymore).
func RunTask(ctx context.Context, st *store.Store, task store.Task) (json.RawMessage, error) {
	p := newProgress()
	var err error
	switch task.Name {
	case "volume.backup":
		task.Archive = resolveArchiveName(task.Archive)
		err = Perform(ctx, st, task, p)
	case "volume.restore":
		err = Restore(ctx, st, task, p)
	case "backup.delete":
		err = DeleteBackup(ctx, st, task, p)
	case "backup.export":
		err = ExportBackup(ctx, st, task, p)
	case "volume.trash":
		err = Trash(ctx, st, task, p)
	default:
		err = fmt.Errorf("unknown task kind %q", task.Name)
	}
	if err == nil && p.Failed() {
		err = errors.New("task reported failure")
	}
	return p.Result(err), err
}

// resolveArchiveName expands the caller-supplied archive name into the
// timestamped form borg records (matching the old job dispatch behavior).
func resolveArchiveName(name string) string {
	switch name {
	case "":
		return "manual-m-{utcnow}"
	case "auto":
		return "auto-{utcnow}"
	default:
		return name + "-m-{utcnow}"
	}
}

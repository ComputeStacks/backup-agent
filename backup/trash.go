package backup

import (
	"context"
	"cs-agent/backup/borg"
	"cs-agent/store"
)

// Trash destroys a volume's borg repository and stops its backup container. The
// scheduler's reconcile enqueues it (a stable, idempotent task id) when a
// volume's desired-state carries Trash=true — replacing the inline teardown the
// old Consul scheduleBackup did. The schedule row is removed by the reconcile;
// the volume's desired-state row is the controller's to delete once it observes
// this task complete (the agent never deletes controller-owned DOWN state).
func Trash(ctx context.Context, st *store.Store, task store.Task, projectEvent *progress) error {
	// No handler-level sentry.Recover(): let a panic reach the worker terminal
	// guard so a crashed teardown is FAILED (never a false "completed").
	repo := borg.Repository{Name: task.Volume, SourceVolumeName: task.Volume, Store: st}
	if _, err := repo.Delete(); err != nil {
		projectEvent.EventLog.Status = "failed"
		projectEvent.PostEventUpdate("agent-volume-trash-failed", err.Error())
		repo.StopContainer()
		return err
	}
	repo.StopContainer()
	backupLogger().Info("Trashed volume repository", "volume", task.Volume)
	return nil
}

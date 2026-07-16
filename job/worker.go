package job

import (
	"context"
	"cs-agent/backup"
	"cs-agent/store"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
)

// worker pulls claimed tasks off its queue and runs them until ctx is cancelled.
func (d *Dispatcher) worker(ctx context.Context, wg *sync.WaitGroup, name string, queue <-chan store.Task) {
	defer wg.Done()
	defer func() { jobEvent().Info("Worker stopping", "queue", name) }()
	for {
		select {
		case <-ctx.Done():
			jobEvent().Info("[" + name + "] Shutting down")
			return
		case task := <-queue:
			d.runTask(ctx, task)
			if ctx.Err() != nil {
				jobEvent().Info("[" + name + "] Shutdown")
				return
			}
		}
	}
}

// runTask executes a claimed (running) task and records its terminal status +
// result via the store. A deferred TERMINAL GUARD ensures a task never stays
// "running": a recovered panic (or any exit without a terminal write) marks the
// task failed — the csevent CloseEvent + finalizeStuckExport that used to own this
// are gone, so the worker owns it now.
func (d *Dispatcher) runTask(ctx context.Context, task store.Task) {
	completed := false
	defer func() {
		if r := recover(); r != nil {
			hub := sentry.CurrentHub().Clone()
			hub.Recover(r)
			hub.Flush(2 * time.Second)
			jobEvent().Error("task panicked", "task", task.ID, "kind", task.Name, "panic", fmt.Sprintf("%v", r))
			d.markFailed(task.ID, "task panicked")
			return
		}
		if !completed {
			// Handler returned without us recording a terminal status (should not
			// happen) — fail closed rather than leave the task running forever.
			d.markFailed(task.ID, "task exited without a terminal result")
		}
	}()

	jobEvent().Info("Processing task", "task", task.ID, "kind", task.Name)
	run := d.runner
	if run == nil {
		run = backup.RunTask
	}
	result, err := run(ctx, d.st, task)
	status := store.TaskCompleted
	if err != nil {
		status = store.TaskFailed
		jobEvent().Warn("task failed", "task", task.ID, "kind", task.Name, "error", err.Error())
	}
	// Record the terminal status on a fresh context, not the worker ctx: a task
	// that finished right as shutdown cancelled ctx must still be recorded with its
	// true outcome (not failed by the guard) — important for the never-replayed
	// kinds (restore/delete/export/trash) where a false failure needs a manual
	// re-request.
	writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if uErr := d.st.UpdateTaskStatus(writeCtx, task.ID, status, result); uErr != nil {
		jobEvent().Warn("failed to record task status", "task", task.ID, "error", uErr.Error())
		return // leave completed=false so the guard marks it failed
	}
	completed = true
}

// markFailed records a failed terminal status on a background context (the
// worker ctx may already be cancelled during shutdown/panic).
func (d *Dispatcher) markFailed(id, reason string) {
	result, _ := json.Marshal(map[string]string{"error": reason})
	if err := d.st.UpdateTaskStatus(context.Background(), id, store.TaskFailed, result); err != nil {
		jobEvent().Warn("failed to mark task failed", "task", id, "error", err.Error())
	}
}

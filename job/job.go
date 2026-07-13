package job

import (
	"context"
	"cs-agent/log"
	"cs-agent/store"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/spf13/viper"
)

// backstopInterval is how often the dispatcher re-drains ListPendingTasks even
// without a wake signal — a safety net for a task committed just before a missed
// signal, or one left pending by an export revert.
const backstopInterval = 30 * time.Second

// Dispatcher is the in-process replacement for the old Consul jobs/ long-poll. A
// SINGLE goroutine drains this node's pending tasks and dispatches each into a
// worker pool; being single-goroutine is load-bearing — it (with the ClaimTask
// CAS) guarantees a task is dispatched at most once even when a wake signal and
// the backstop coincide.
type Dispatcher struct {
	st            *store.Store
	hostname      string
	backupQ       chan store.Task
	exportQ       chan store.Task
	signal        chan struct{}
	backupWorkers int
	exportWorkers int
}

// NewDispatcher builds the dispatcher (unbuffered worker queues sized by config).
func NewDispatcher(st *store.Store) *Dispatcher {
	hostname, _ := os.Hostname()
	backupWorkers := viper.GetInt("queue.numworkers") + 1
	if backupWorkers < 1 {
		backupWorkers = 1
	}
	exportWorkers := viper.GetInt("backups.export.workers")
	if exportWorkers < 1 {
		exportWorkers = 1
	}
	return &Dispatcher{
		st:            st,
		hostname:      hostname,
		backupQ:       make(chan store.Task),
		exportQ:       make(chan store.Task),
		signal:        make(chan struct{}, 1),
		backupWorkers: backupWorkers,
		exportWorkers: exportWorkers,
	}
}

// Signal wakes the dispatcher to drain pending tasks. Non-blocking + coalescing:
// callers (the task-create HTTP handler, the scheduler) never block, and bursts
// collapse into a single drain.
func (d *Dispatcher) Signal() {
	select {
	case d.signal <- struct{}{}:
	default:
	}
}

// Start runs the boot crash-reconcile, starts the worker pools, and runs the
// single dispatch loop until ctx is done. Call in its own goroutine. wg tracks
// the worker pools + the dispatch loop so main can bound the shutdown drain.
func (d *Dispatcher) Start(ctx context.Context, wg *sync.WaitGroup) {
	// Fail any task left "running" by a crashed process BEFORE accepting new
	// work, so a re-drain can't race a reconcile of the same task.
	d.bootReconcile(ctx)

	d.startWorkers(ctx, wg, "backup", d.backupWorkers, d.backupQ)
	d.startWorkers(ctx, wg, "export", d.exportWorkers, d.exportQ)

	wg.Add(1)
	go d.loop(ctx, wg)
}

func (d *Dispatcher) startWorkers(ctx context.Context, wg *sync.WaitGroup, name string, count int, q <-chan store.Task) {
	wg.Add(count)
	for i := 1; i <= count; i++ {
		jobEvent().Info("Starting worker process", "queue", name, "worker-process", i)
		go d.worker(ctx, wg, name, q)
	}
}

// loop is the single dispatch goroutine.
func (d *Dispatcher) loop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() { jobEvent().Info("Dispatcher stopping") }()

	d.drain(ctx) // drain whatever is already pending at boot
	backstop := time.NewTicker(backstopInterval)
	defer backstop.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.signal:
			d.drain(ctx)
		case <-backstop.C:
			d.drain(ctx)
		}
	}
}

// drain claims and dispatches every pending task for this node. It is the ONLY
// claimer/dispatcher. Exports are dispatched first (non-blocking) so a full backup
// queue can't head-of-line-block an export; backups then send blocking (the
// workers are the throughput limiter).
func (d *Dispatcher) drain(ctx context.Context) {
	pending, err := d.st.ListPendingTasks(ctx, d.hostname)
	if err != nil {
		jobEvent().Warn("dispatch: list pending tasks", "error", err.Error())
		return
	}
	for _, task := range pending {
		if ctx.Err() != nil {
			return
		}
		if task.Name == "backup.export" {
			d.dispatchExport(ctx, task)
		}
	}
	for _, task := range pending {
		if ctx.Err() != nil {
			return
		}
		if task.Name != "backup.export" {
			d.dispatchBackup(ctx, task)
		}
	}
}

// dispatchBackup claims (CAS pending->running) then blocking-sends to the backup
// pool. Only a task we won the CAS on is dispatched, so a signal + backstop can't
// double-run one task.
func (d *Dispatcher) dispatchBackup(ctx context.Context, task store.Task) {
	claimed, err := d.st.ClaimTask(ctx, task.ID)
	if err != nil {
		jobEvent().Warn("dispatch: claim task", "task", task.ID, "error", err.Error())
		return
	}
	if !claimed {
		return // already claimed/terminal
	}
	select {
	case d.backupQ <- task:
	case <-ctx.Done():
	}
}

// dispatchExport claims then NON-BLOCKING sends to the export pool; if the pool is
// full the claim is reverted (running->pending) so a later wake retries it. A
// crash between claim and revert leaves the task running, which the boot reconcile
// then fails — an export is never blindly re-run.
func (d *Dispatcher) dispatchExport(ctx context.Context, task store.Task) {
	claimed, err := d.st.ClaimTask(ctx, task.ID)
	if err != nil {
		jobEvent().Warn("dispatch: claim export", "task", task.ID, "error", err.Error())
		return
	}
	if !claimed {
		return
	}
	select {
	case d.exportQ <- task:
	default:
		if _, uErr := d.st.UnclaimTask(ctx, task.ID); uErr != nil {
			jobEvent().Warn("dispatch: unclaim export (pool full)", "task", task.ID, "error", uErr.Error())
		}
	}
}

// bootReconcile fails every task left "running" by a crashed process. On boot no
// task is truly in flight, so a "running" row is orphaned work. NONE are
// auto-replayed — destructive kinds (restore/delete/trash) must never re-run
// unbidden, and export must never blindly re-upload; the scheduler re-fires
// backups on their next slot; the controller re-requests the rest.
func (d *Dispatcher) bootReconcile(ctx context.Context) {
	running, err := d.st.ListRunningTasks(ctx, d.hostname)
	if err != nil {
		jobEvent().Warn("boot reconcile: list running tasks", "error", err.Error())
		return
	}
	result, _ := json.Marshal(map[string]string{"error": "agent restarted while task was running; not auto-replayed"})
	for _, task := range running {
		if err := d.st.UpdateTaskStatus(ctx, task.ID, store.TaskFailed, result); err != nil {
			jobEvent().Warn("boot reconcile: mark failed", "task", task.ID, "error", err.Error())
			continue
		}
		jobEvent().Warn("boot reconcile: failed orphaned running task", "task", task.ID, "kind", task.Name)
	}
}

func jobEvent() hclog.Logger {
	return log.New().Named("worker")
}

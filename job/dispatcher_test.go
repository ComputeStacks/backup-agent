package job

import (
	"context"
	"testing"
	"time"

	"cs-agent/store"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func newTestDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	return NewDispatcher(testStore(t))
}

// TestDispatcher_DrainClaimsOnce proves the load-bearing guarantee: drain claims a
// pending task (pending->running) and dispatches it exactly once; a second drain
// sees it running and dispatches nothing.
func TestDispatcher_DrainClaimsOnce(t *testing.T) {
	d := newTestDispatcher(t)
	ctx := context.Background()
	if _, err := d.st.CreateTask(ctx, store.Task{ID: "b1", Name: "volume.backup", Node: "test-node"}); err != nil {
		t.Fatal(err)
	}

	// A consumer for the (unbuffered) backup queue.
	got := make(chan store.Task, 2)
	go func() {
		for task := range d.backupQ {
			got <- task
		}
	}()

	d.drain(ctx)
	select {
	case task := <-got:
		if task.ID != "b1" {
			t.Fatalf("dispatched %q, want b1", task.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task was not dispatched")
	}
	if tk, _, _ := d.st.GetTask(ctx, "b1"); tk.Status != store.TaskRunning {
		t.Fatalf("status = %q, want running", tk.Status)
	}

	// Second drain: task is no longer pending, so nothing is dispatched.
	d.drain(ctx)
	select {
	case task := <-got:
		t.Fatalf("task re-dispatched: %q", task.ID)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestDispatcher_ExportRevertsWhenPoolFull proves an export claimed while the
// export pool is full is reverted to pending (not left stuck running), so a later
// wake retries it.
func TestDispatcher_ExportRevertsWhenPoolFull(t *testing.T) {
	d := newTestDispatcher(t)
	ctx := context.Background()
	if _, err := d.st.CreateTask(ctx, store.Task{ID: "e1", Name: "backup.export", Node: "test-node"}); err != nil {
		t.Fatal(err)
	}
	// No consumer on exportQ (unbuffered) => the non-blocking send fails => revert.
	d.drain(ctx)
	tk, _, _ := d.st.GetTask(ctx, "e1")
	if tk.Status != store.TaskPending {
		t.Fatalf("status = %q, want pending (reverted)", tk.Status)
	}
}

// TestDispatcher_BootReconcileFailsRunning proves a task left running by a crash is
// failed on boot and never auto-replayed.
func TestDispatcher_BootReconcileFailsRunning(t *testing.T) {
	d := newTestDispatcher(t)
	ctx := context.Background()
	for _, kind := range []string{"volume.backup", "volume.restore", "backup.export", "volume.trash"} {
		id := kind
		if _, err := d.st.CreateTask(ctx, store.Task{ID: id, Name: kind, Node: "test-node"}); err != nil {
			t.Fatal(err)
		}
		if _, err := d.st.ClaimTask(ctx, id); err != nil { // -> running
			t.Fatal(err)
		}
	}
	d.bootReconcile(ctx)
	if running, _ := d.st.ListRunningTasks(ctx); len(running) != 0 {
		t.Fatalf("running after boot reconcile = %d, want 0", len(running))
	}
	if pending, _ := d.st.ListPendingTasks(ctx); len(pending) != 0 {
		t.Fatalf("pending after boot reconcile = %d, want 0 (nothing auto-replayed)", len(pending))
	}
	tk, _, _ := d.st.GetTask(ctx, "volume.restore")
	if tk.Status != store.TaskFailed {
		t.Fatalf("restore status = %q, want failed", tk.Status)
	}
}

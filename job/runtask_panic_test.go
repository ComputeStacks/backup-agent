package job

import (
	"context"
	"encoding/json"
	"testing"

	"cs-agent/store"
)

// TestRunTask_PanicMarksFailed proves the worker terminal guard converts a panic in
// the task runner into a FAILED task (never a stuck "running" or a false
// "completed"). Together with backup.TestTaskHandlersHaveNoRecover (which ensures
// the panic actually reaches this guard), this covers the B1 regression.
func TestRunTask_PanicMarksFailed(t *testing.T) {
	d := newTestDispatcher(t)
	ctx := context.Background()
	if _, err := d.st.CreateTask(ctx, store.Task{ID: "p1", Name: "volume.restore", Node: "test-node"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.st.ClaimTask(ctx, "p1"); err != nil { // -> running, as a worker receives it
		t.Fatal(err)
	}
	d.runner = func(context.Context, *store.Store, store.Task) (json.RawMessage, error) {
		panic("boom")
	}

	d.runTask(ctx, store.Task{ID: "p1", Name: "volume.restore", Node: "test-node"})

	tk, found, err := d.st.GetTask(ctx, "p1")
	if err != nil || !found {
		t.Fatalf("get task: found=%v err=%v", found, err)
	}
	if tk.Status != store.TaskFailed {
		t.Fatalf("status = %q, want failed after a runner panic", tk.Status)
	}
}

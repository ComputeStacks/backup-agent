package store

import (
	"encoding/json"
	"testing"
)

// TestEnqueueTeardown covers the M2 fix: idempotent teardown enqueue that, unlike
// CreateTask's ON CONFLICT DO NOTHING, can re-activate a *failed* prior attempt —
// but only on the controller-driven DELETE path (resetFailed=true), never on the
// scheduler reconcile path (resetFailed=false), which would be a tick-rate loop.
func TestEnqueueTeardown(t *testing.T) {
	base := Task{ID: "volume.trash:v1", Name: "volume.trash", Node: "node-a", Volume: "v1", ProjectID: "p1"}

	t.Run("absent inserts pending", func(t *testing.T) {
		s := open(t, Options{})
		enq, err := s.EnqueueTeardown(ctx, base, false)
		if err != nil || !enq {
			t.Fatalf("enqueue: enq=%v err=%v", enq, err)
		}
		tk, found, _ := s.GetTask(ctx, base.ID)
		if !found || tk.Status != TaskPending {
			t.Fatalf("task: found=%v status=%q", found, tk.Status)
		}
	})

	t.Run("pending is a no-op", func(t *testing.T) {
		s := open(t, Options{})
		mustEnqueue(t, s, base)
		enq, err := s.EnqueueTeardown(ctx, base, true) // even resetFailed=true
		if err != nil || enq {
			t.Fatalf("re-enqueue pending: enq=%v err=%v (want no-op)", enq, err)
		}
	})

	t.Run("running is a no-op", func(t *testing.T) {
		s := open(t, Options{})
		mustEnqueue(t, s, base)
		if _, err := s.ClaimTask(ctx, base.ID); err != nil { // -> running
			t.Fatal(err)
		}
		enq, err := s.EnqueueTeardown(ctx, base, true)
		if err != nil || enq {
			t.Fatalf("re-enqueue running: enq=%v err=%v (want no-op)", enq, err)
		}
		if tk, _, _ := s.GetTask(ctx, base.ID); tk.Status != TaskRunning {
			t.Fatalf("status = %q, want running (unchanged)", tk.Status)
		}
	})

	t.Run("completed is a no-op", func(t *testing.T) {
		s := open(t, Options{})
		mustEnqueue(t, s, base)
		if err := s.UpdateTaskStatus(ctx, base.ID, TaskCompleted, nil); err != nil {
			t.Fatal(err)
		}
		enq, err := s.EnqueueTeardown(ctx, base, true)
		if err != nil || enq {
			t.Fatalf("re-enqueue completed: enq=%v err=%v (want no-op)", enq, err)
		}
		if tk, _, _ := s.GetTask(ctx, base.ID); tk.Status != TaskCompleted {
			t.Fatalf("status = %q, want completed (unchanged)", tk.Status)
		}
	})

	t.Run("failed reconcile path (resetFailed=false) stays failed", func(t *testing.T) {
		s := open(t, Options{})
		mustEnqueue(t, s, base)
		if err := s.UpdateTaskStatus(ctx, base.ID, TaskFailed, nil); err != nil {
			t.Fatal(err)
		}
		enq, err := s.EnqueueTeardown(ctx, base, false)
		if err != nil || enq {
			t.Fatalf("reconcile re-enqueue of failed: enq=%v err=%v (want no-op — no tick-loop retry)", enq, err)
		}
		if tk, _, _ := s.GetTask(ctx, base.ID); tk.Status != TaskFailed {
			t.Fatalf("status = %q, want failed (reconcile must not reset)", tk.Status)
		}
	})

	t.Run("failed DELETE path (resetFailed=true) resets to pending", func(t *testing.T) {
		s := open(t, Options{})
		mustEnqueue(t, s, base)
		if err := s.UpdateTaskStatus(ctx, base.ID, TaskFailed, json.RawMessage(`{"error":"boom"}`)); err != nil {
			t.Fatal(err)
		}
		enq, err := s.EnqueueTeardown(ctx, base, true)
		if err != nil || !enq {
			t.Fatalf("DELETE re-enqueue of failed: enq=%v err=%v (want reset+enqueue)", enq, err)
		}
		tk, _, _ := s.GetTask(ctx, base.ID)
		if tk.Status != TaskPending {
			t.Fatalf("status = %q, want pending (reset for retry)", tk.Status)
		}
		if len(tk.Result) != 0 {
			t.Fatalf("result_json = %s, want cleared on reset", tk.Result)
		}
	})
}

func mustEnqueue(t *testing.T, s *Store, task Task) {
	t.Helper()
	if _, err := s.EnqueueTeardown(ctx, task, false); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}
}

package backup

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"cs-agent/store"
	"cs-agent/types"
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

func putVol(t *testing.T, st *store.Store, v types.Volume) {
	t.Helper()
	if err := st.PutVolume(context.Background(), store.Volume{
		Name:      v.Name,
		Node:      v.Node,
		ProjectID: strconv.Itoa(v.ProjectID),
		Config:    json.RawMessage(v.JSONEncode()),
	}); err != nil {
		t.Fatalf("put volume %q: %v", v.Name, err)
	}
}

func newTestScheduler(t *testing.T, st *store.Store) *Scheduler {
	t.Helper()
	s := NewScheduler(st, nil)
	s.hostname = "test-node"
	return s
}

func TestScheduler_Reconcile(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	s := newTestScheduler(t, st)

	// Unpopulated control.db: reconcile must be a no-op (never wipe schedules).
	// (No PutVolume yet, so the volumes-populated sentinel is unset.)
	s.reconcile(ctx)
	if all, _ := st.ListSchedules(ctx); len(all) != 0 {
		t.Fatalf("schedules created while unpopulated: %d", len(all))
	}

	// Backup-enabled volume -> a schedule with a FUTURE next_fire_at (no storm).
	putVol(t, st, types.Volume{Name: "v1", Node: "test-node", Backup: true, Freq: "0 2 * * *", ProjectID: 7})
	s.reconcile(ctx)
	sc, found, _ := st.GetSchedule(ctx, "v1")
	if !found || sc.CronExpr != "0 2 * * *" {
		t.Fatalf("schedule for v1: found=%v %+v", found, sc)
	}
	if sc.NextFireAt <= time.Now().Unix() {
		t.Fatalf("fresh next_fire_at = %d is not in the future (would storm on backfill)", sc.NextFireAt)
	}

	// Backup-disabled volume -> no schedule.
	putVol(t, st, types.Volume{Name: "v2", Node: "test-node", Backup: false, ProjectID: 7})
	s.reconcile(ctx)
	if _, found, _ := st.GetSchedule(ctx, "v2"); found {
		t.Fatal("schedule created for a backup-disabled volume")
	}

	// Trash the volume -> a stable-id volume.trash task + schedule removed.
	putVol(t, st, types.Volume{Name: "v1", Node: "test-node", Backup: true, Freq: "0 2 * * *", Trash: true, ProjectID: 7})
	s.reconcile(ctx)
	if _, found, _ := st.GetSchedule(ctx, "v1"); found {
		t.Fatal("schedule not removed for a trashed volume")
	}
	tk, found, _ := st.GetTask(ctx, "volume.trash:v1")
	if !found || tk.Name != "volume.trash" || tk.Volume != "v1" {
		t.Fatalf("trash task: found=%v %+v", found, tk)
	}
	// Idempotent: a second reconcile does not enqueue a duplicate trash task.
	s.reconcile(ctx)
	if got := countPendingTrash(t, st); got != 1 {
		t.Fatalf("pending trash tasks = %d, want 1 (idempotent)", got)
	}
}

func countPendingTrash(t *testing.T, st *store.Store) int {
	t.Helper()
	pending, err := st.ListPendingTasks(context.Background(), "test-node")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, p := range pending {
		if p.Name == "volume.trash" {
			n++
		}
	}
	return n
}

func TestScheduler_FireDue(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	s := newTestScheduler(t, st)
	putVol(t, st, types.Volume{Name: "v1", Node: "test-node", Backup: true, Freq: "0 2 * * *", ProjectID: 7})
	// A schedule already due (next_fire_at in the past).
	if err := st.PutSchedule(ctx, "v1", "0 2 * * *", 1); err != nil {
		t.Fatal(err)
	}
	s.fireDue(ctx)

	pending, _ := st.ListPendingTasks(ctx, "test-node")
	if len(pending) != 1 || pending[0].Name != "volume.backup" || pending[0].Volume != "v1" {
		t.Fatalf("fireDue pending tasks: %+v", pending)
	}
	// next_fire_at advanced into the future -> no longer due (exactly-once).
	sc, _, _ := st.GetSchedule(ctx, "v1")
	if sc.NextFireAt <= time.Now().Unix() {
		t.Fatalf("next_fire_at = %d not advanced past now", sc.NextFireAt)
	}
}

func TestRunTask_UnknownKind(t *testing.T) {
	st := testStore(t)
	result, err := RunTask(context.Background(), st, store.Task{ID: "x", Name: "bogus.kind", Node: "n"})
	if err == nil {
		t.Fatal("unknown kind did not error")
	}
	var m map[string]any
	if uErr := json.Unmarshal(result, &m); uErr != nil {
		t.Fatalf("result not JSON: %v", uErr)
	}
	if m["error"] == nil {
		t.Fatalf("result missing error field: %s", result)
	}
}

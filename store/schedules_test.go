package store

import (
	"testing"
)

func TestControlMigrations_V4(t *testing.T) {
	s := open(t, Options{})
	if got := countTable(t, s, "schedules"); got != 0 {
		t.Fatalf("schedules not empty on fresh open: %d", got)
	}
	// The convergence bake-in columns must exist (query errors if a column is absent).
	for _, q := range []string{
		`SELECT count(generation), count(applied_generation) FROM volumes`,
		`SELECT count(generation), count(applied_generation) FROM firewall_rules`,
	} {
		var a, b int
		if err := s.control.QueryRowContext(ctx, q).Scan(&a, &b); err != nil {
			t.Fatalf("convergence columns missing (%s): %v", q, err)
		}
	}
}

func TestScheduleCRUD(t *testing.T) {
	s := open(t, Options{})
	if _, found, err := s.GetSchedule(ctx, "vol-1"); err != nil || found {
		t.Fatalf("get missing: found=%v err=%v", found, err)
	}
	if err := s.PutSchedule(ctx, "vol-1", "0 2 * * *", 1000); err != nil {
		t.Fatalf("PutSchedule: %v", err)
	}
	sc, found, err := s.GetSchedule(ctx, "vol-1")
	if err != nil || !found {
		t.Fatalf("GetSchedule: found=%v err=%v", found, err)
	}
	if sc.CronExpr != "0 2 * * *" || sc.NextFireAt != 1000 {
		t.Fatalf("schedule: %+v", sc)
	}
	// A reschedule (cron changed) overwrites cron_expr + next_fire_at.
	if err := s.PutSchedule(ctx, "vol-1", "0 5 * * *", 2000); err != nil {
		t.Fatal(err)
	}
	sc, _, _ = s.GetSchedule(ctx, "vol-1")
	if sc.CronExpr != "0 5 * * *" || sc.NextFireAt != 2000 {
		t.Fatalf("after reschedule: %+v", sc)
	}
	// Schedules are node-local state, never changelogged.
	if got := countTable(t, s, "changelog"); got != 0 {
		t.Fatalf("changelog = %d, want 0 (schedules must not changelog)", got)
	}
	if err := s.DeleteSchedule(ctx, "vol-1"); err != nil {
		t.Fatalf("DeleteSchedule: %v", err)
	}
	if got := countTable(t, s, "schedules"); got != 0 {
		t.Fatalf("schedules = %d, want 0", got)
	}
	// Deleting an absent schedule is a no-op.
	if err := s.DeleteSchedule(ctx, "gone"); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
}

func TestListDueSchedules(t *testing.T) {
	s := open(t, Options{})
	if err := s.PutSchedule(ctx, "due-1", "* * * * *", 100); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSchedule(ctx, "due-2", "* * * * *", 150); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSchedule(ctx, "future", "* * * * *", 500); err != nil {
		t.Fatal(err)
	}
	due, err := s.ListDueSchedules(ctx, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 2 {
		t.Fatalf("due = %d, want 2", len(due))
	}
	// Oldest-due first.
	if due[0].VolumeName != "due-1" || due[1].VolumeName != "due-2" {
		t.Fatalf("due order: %+v", due)
	}
	if all, _ := s.ListSchedules(ctx); len(all) != 3 {
		t.Fatalf("ListSchedules = %d, want 3", len(all))
	}
}

// TestFireDueBackup_ExactlyOnce proves the SQLite payoff: firing a due schedule
// creates the task AND advances next_fire_at in one transaction, so the same
// schedule is no longer due afterward (no double fire) — and the schedule advance
// is not changelogged while the task creation is.
func TestFireDueBackup_ExactlyOnce(t *testing.T) {
	s := open(t, Options{})
	if err := s.PutSchedule(ctx, "vol-1", "0 2 * * *", 100); err != nil {
		t.Fatal(err)
	}
	created, err := s.FireDueBackup(ctx, Task{
		ID: "auto-1", Name: "volume.backup", Node: "node-a", Volume: "vol-1", ProjectID: "proj-1",
	}, 3700)
	if err != nil || !created {
		t.Fatalf("FireDueBackup: created=%v err=%v", created, err)
	}
	// Task exists, pending.
	tk, found, _ := s.GetTask(ctx, "auto-1")
	if !found || tk.Status != TaskPending || tk.Volume != "vol-1" {
		t.Fatalf("fired task: found=%v %+v", found, tk)
	}
	// next_fire_at advanced past the old due time -> no longer due at t=200.
	sc, _, _ := s.GetSchedule(ctx, "vol-1")
	if sc.NextFireAt != 3700 {
		t.Fatalf("next_fire_at = %d, want 3700", sc.NextFireAt)
	}
	if due, _ := s.ListDueSchedules(ctx, 200); len(due) != 0 {
		t.Fatalf("still due after fire: %d", len(due))
	}
	// The task creation is changelogged; the schedule advance is not (1 row).
	if got := countTable(t, s, "changelog"); got != 1 {
		t.Fatalf("changelog = %d, want 1 (task upsert only)", got)
	}
}

func TestClaimTask_CAS(t *testing.T) {
	s := open(t, Options{})
	if _, err := s.CreateTask(ctx, Task{ID: "j1", Name: "volume.backup", Node: "n"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimTask(ctx, "j1")
	if err != nil || !claimed {
		t.Fatalf("first claim: claimed=%v err=%v", claimed, err)
	}
	got, _, _ := s.GetTask(ctx, "j1")
	if got.Status != TaskRunning {
		t.Fatalf("status = %q, want running", got.Status)
	}
	// A second claim (already running) is a no-op — this is what prevents a wake
	// signal and the backstop drain from double-dispatching one task.
	claimed, err = s.ClaimTask(ctx, "j1")
	if err != nil || claimed {
		t.Fatalf("second claim: claimed=%v err=%v, want false", claimed, err)
	}
	// create + claim = 2 changelog rows (the no-op second claim adds none).
	if got := countTable(t, s, "changelog"); got != 2 {
		t.Fatalf("changelog = %d, want 2", got)
	}
	if c, err := s.ClaimTask(ctx, "absent"); err != nil || c {
		t.Fatalf("claim absent: c=%v err=%v", c, err)
	}
}

func TestUnclaimTask_RevertsRunningOnly(t *testing.T) {
	s := open(t, Options{})
	if _, err := s.CreateTask(ctx, Task{ID: "e1", Name: "backup.export", Node: "n"}); err != nil {
		t.Fatal(err)
	}
	// Cannot unclaim a pending task (only running).
	if u, err := s.UnclaimTask(ctx, "e1"); err != nil || u {
		t.Fatalf("unclaim pending: u=%v err=%v, want false", u, err)
	}
	if _, err := s.ClaimTask(ctx, "e1"); err != nil {
		t.Fatal(err)
	}
	// Now running -> unclaim reverts to pending (export pool was full).
	u, err := s.UnclaimTask(ctx, "e1")
	if err != nil || !u {
		t.Fatalf("unclaim running: u=%v err=%v", u, err)
	}
	got, _, _ := s.GetTask(ctx, "e1")
	if got.Status != TaskPending {
		t.Fatalf("status = %q, want pending", got.Status)
	}
	// A terminal task can't be unclaimed (guards against stomping a finished export).
	if _, err := s.ClaimTask(ctx, "e1"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "e1", TaskCompleted, nil); err != nil {
		t.Fatal(err)
	}
	if u, err := s.UnclaimTask(ctx, "e1"); err != nil || u {
		t.Fatalf("unclaim completed: u=%v err=%v, want false", u, err)
	}
}

func TestListRunningTasks(t *testing.T) {
	s := open(t, Options{})
	for _, id := range []string{"r1", "r2", "r3"} {
		if _, err := s.CreateTask(ctx, Task{ID: id, Name: "volume.backup", Node: "node-a"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ClaimTask(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimTask(ctx, "r2"); err != nil {
		t.Fatal(err)
	}
	// A different node label is still returned: the DB is the node scope, so
	// ListRunningTasks does not filter by node.
	if _, err := s.CreateTask(ctx, Task{ID: "x1", Name: "volume.backup", Node: "node-b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimTask(ctx, "x1"); err != nil {
		t.Fatal(err)
	}
	running, err := s.ListRunningTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 3 {
		t.Fatalf("running = %d, want 3 (all running regardless of node label)", len(running))
	}
}

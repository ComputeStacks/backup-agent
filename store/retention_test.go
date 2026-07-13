package store

import (
	"sort"
	"testing"
)

// insertChangelogRow writes a synthetic changelog row with an explicit seq +
// created_at so prune-by-age is testable without waiting real time.
func insertChangelogRow(t *testing.T, s *Store, seq, createdAt int64) {
	t.Helper()
	if _, err := s.control.ExecContext(ctx,
		`INSERT INTO changelog (seq, entity_type, entity_id, op, created_at) VALUES (?, 'task', 'e', 'upsert', ?)`,
		seq, createdAt); err != nil {
		t.Fatalf("insert changelog seq=%d: %v", seq, err)
	}
}

func remainingSeqs(t *testing.T, s *Store) []int64 {
	t.Helper()
	rows, err := s.control.QueryContext(ctx, `SELECT seq FROM changelog ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatal(err)
		}
		out = append(out, seq)
	}
	return out
}

func TestPruneChangelog_AckGatedAndFallback(t *testing.T) {
	s := open(t, Options{})
	// (seq, created_at)
	insertChangelogRow(t, s, 1, 8000)  // old: reaped by age fallback
	insertChangelogRow(t, s, 2, 9950)  // recent: kept (newer than both cutoffs)
	insertChangelogRow(t, s, 3, 9500)  // acked (3<=5) + older than minCutoff: reaped
	insertChangelogRow(t, s, 10, 9500) // unacked (10>5) + newer than maxCutoff: kept
	if err := s.SetChangelogAcked(ctx, 5); err != nil {
		t.Fatal(err)
	}
	// now=10000, minAge=100 -> minCutoff 9900; maxAge=1000 -> maxCutoff 9000.
	deleted, err := s.PruneChangelog(ctx, 10000, 100, 1000)
	if err != nil {
		t.Fatalf("PruneChangelog: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	got := remainingSeqs(t, s)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if len(got) != 2 || got[0] != 2 || got[1] != 10 {
		t.Fatalf("remaining seqs = %v, want [2 10]", got)
	}
}

// TestPruneChangelog_FallbackBoundsPreIntegrationGap proves the MAJOR-3 fix: with
// no controller acking yet (acked=0), old rows are still bounded by the age
// fallback, and disabling the fallback (maxAge<=0) leaves them.
func TestPruneChangelog_FallbackBoundsPreIntegrationGap(t *testing.T) {
	s := open(t, Options{})
	insertChangelogRow(t, s, 1, 100) // very old, never acked
	// Fallback disabled: nothing acked, nothing pruned.
	if deleted, err := s.PruneChangelog(ctx, 100000, 100, 0); err != nil || deleted != 0 {
		t.Fatalf("fallback disabled: deleted=%d err=%v, want 0", deleted, err)
	}
	// Fallback enabled: the old row is reaped despite acked=0.
	if deleted, err := s.PruneChangelog(ctx, 100000, 100, 1000); err != nil || deleted != 1 {
		t.Fatalf("fallback enabled: deleted=%d err=%v, want 1", deleted, err)
	}
}

func TestSetChangelogAcked_Monotonic(t *testing.T) {
	s := open(t, Options{})
	if got, _ := s.GetChangelogAcked(ctx); got != 0 {
		t.Fatalf("initial acked = %d, want 0", got)
	}
	if err := s.SetChangelogAcked(ctx, 5); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetChangelogAcked(ctx); got != 5 {
		t.Fatalf("acked = %d, want 5", got)
	}
	// A rewind is silently ignored (monotonic guard).
	if err := s.SetChangelogAcked(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetChangelogAcked(ctx); got != 5 {
		t.Fatalf("after rewind attempt acked = %d, want 5", got)
	}
	// Advance forward.
	if err := s.SetChangelogAcked(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetChangelogAcked(ctx); got != 10 {
		t.Fatalf("after advance acked = %d, want 10", got)
	}
}

func TestDeleteTerminalTasksBefore(t *testing.T) {
	s := open(t, Options{})
	for _, id := range []string{"pending", "done", "failed"} {
		if _, err := s.CreateTask(ctx, Task{ID: id, Name: "volume.backup", Node: "n"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.UpdateTaskStatus(ctx, "done", TaskCompleted, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "failed", TaskFailed, nil); err != nil {
		t.Fatal(err)
	}
	// Age the two terminal tasks into the past.
	if _, err := s.control.ExecContext(ctx,
		`UPDATE tasks SET updated_at = 1000 WHERE id IN ('done','failed')`); err != nil {
		t.Fatal(err)
	}
	deleted, err := s.DeleteTerminalTasksBefore(ctx, 2000)
	if err != nil {
		t.Fatalf("DeleteTerminalTasksBefore: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2 (done+failed)", deleted)
	}
	// The pending task survives (never a reap candidate).
	if got := countTable(t, s, "tasks"); got != 1 {
		t.Fatalf("tasks = %d, want 1 (pending)", got)
	}
	if _, found, _ := s.GetTask(ctx, "pending"); !found {
		t.Fatal("pending task was reaped")
	}
}

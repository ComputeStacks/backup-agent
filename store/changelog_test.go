package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// countTable returns the row count of a control.db table (white-box helper).
func countTable(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	if err := s.control.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestControlMigrations_V2_Tables(t *testing.T) {
	s := open(t, Options{})
	for _, tbl := range []string{"changelog", "action_requests"} {
		if got := countTable(t, s, tbl); got != 0 {
			t.Fatalf("%s not empty on fresh open: %d", tbl, got)
		}
	}
}

func TestCreateActionRequest_AppendsChangelog(t *testing.T) {
	s := open(t, Options{})

	params := json.RawMessage(`{"paths":["/a","/b/*"]}`)
	ar, err := s.CreateActionRequest(ctx, "act-1", "proj-1", "cdn_purge", params)
	if err != nil {
		t.Fatalf("CreateActionRequest: %v", err)
	}
	if ar.Status != "pending" || ar.CreatedAt == 0 || ar.UpdatedAt == 0 {
		t.Fatalf("unexpected action request: %+v", ar)
	}
	if got := countTable(t, s, "action_requests"); got != 1 {
		t.Fatalf("action_requests = %d, want 1", got)
	}

	var (
		seq                                       int64
		entityType, entityID, projectID, op, blob string
		typ                                       string
	)
	if err := s.control.QueryRowContext(ctx,
		`SELECT seq, entity_type, entity_id, project_id, op, payload, typeof(payload) FROM changelog`,
	).Scan(&seq, &entityType, &entityID, &projectID, &op, &blob, &typ); err != nil {
		t.Fatalf("changelog row: %v", err)
	}
	if seq != 1 || entityType != "action_request" || entityID != "act-1" || projectID != "proj-1" || op != "upsert" {
		t.Fatalf("changelog row = seq %d type %q id %q proj %q op %q", seq, entityType, entityID, projectID, op)
	}
	// payload must be TEXT storage class (not BLOB), so json1 sees JSON text.
	if typ != "text" {
		t.Fatalf("payload typeof = %q, want text", typ)
	}
	var snap ActionRequest
	if err := json.Unmarshal([]byte(blob), &snap); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if snap.ID != "act-1" || snap.ActionType != "cdn_purge" {
		t.Fatalf("snapshot mismatch: %+v", snap)
	}
}

// TestWithControlTx_RollbackOnError proves a *written* changelog row is rolled
// back when the tx's fn fails after the append (the duplicate-id path fails on
// the first statement and never exercises this).
func TestWithControlTx_RollbackOnError(t *testing.T) {
	s := open(t, Options{})
	boom := errors.New("boom")
	err := s.withControlTx(ctx, func(tx *sql.Tx) error {
		if e := appendChangelogTx(ctx, tx, "action_request", "x", "proj-1", "upsert", []byte(`{"id":"x"}`)); e != nil {
			return e
		}
		return boom // fail AFTER the changelog row was written
	})
	if !errors.Is(err, boom) {
		t.Fatalf("withControlTx err = %v, want boom", err)
	}
	if got := countTable(t, s, "changelog"); got != 0 {
		t.Fatalf("changelog has %d rows after rollback, want 0", got)
	}
}

// TestCreateActionRequest_DuplicateID: a PK conflict rolls back the whole tx, so
// no orphan changelog row is left behind.
func TestCreateActionRequest_DuplicateID(t *testing.T) {
	s := open(t, Options{})
	if _, err := s.CreateActionRequest(ctx, "dup", "proj-1", "cdn_purge", nil); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err := s.CreateActionRequest(ctx, "dup", "proj-1", "cdn_purge", nil); err == nil {
		t.Fatal("expected error on duplicate id")
	}
	if got := countTable(t, s, "action_requests"); got != 1 {
		t.Fatalf("action_requests = %d, want 1", got)
	}
	if got := countTable(t, s, "changelog"); got != 1 {
		t.Fatalf("changelog = %d, want 1 (no orphan from the failed insert)", got)
	}
}

// TestCreateActionRequest_ConcurrentSeqContiguous is the load-bearing test: it
// proves the changelog seq stays gap-free and contiguous under concurrent
// writers (run with -race). All inserts commit, so seqs must be exactly 1..N,
// and a poller reading "seq > cursor" observes the complete, strictly-increasing
// sequence.
func TestCreateActionRequest_ConcurrentSeqContiguous(t *testing.T) {
	s := open(t, Options{})
	const n = 50

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.CreateActionRequest(ctx, fmt.Sprintf("act-%d", i), "proj-1", "cdn_purge", nil); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent CreateActionRequest: %v", err)
	}

	if got := countTable(t, s, "action_requests"); got != n {
		t.Fatalf("action_requests = %d, want %d", got, n)
	}

	rows, err := s.control.QueryContext(ctx, "SELECT seq FROM changelog ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var seqs []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	if len(seqs) != n {
		t.Fatalf("changelog rows = %d, want %d", len(seqs), n)
	}
	for i, seq := range seqs {
		if seq != int64(i+1) {
			t.Fatalf("seq[%d] = %d, want %d (non-contiguous)", i, seq, i+1)
		}
	}

	// Poller view: draining "seq > cursor" yields the full, in-order sequence.
	var cursor, seen int64
	for {
		entries, err := s.ChangelogSince(ctx, cursor, "action_request", 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			if e.Seq != seen+1 {
				t.Fatalf("poller saw seq %d, expected %d", e.Seq, seen+1)
			}
			seen = e.Seq
		}
		cursor = entries[len(entries)-1].Seq
	}
	if seen != int64(n) {
		t.Fatalf("poller drained up to seq %d, want %d", seen, n)
	}
}

func TestChangelogSince_PaginationAndFilter(t *testing.T) {
	s := open(t, Options{})
	for i := 1; i <= 3; i++ {
		if _, err := s.CreateActionRequest(ctx, fmt.Sprintf("a-%d", i), "proj-1", "cdn_purge", nil); err != nil {
			t.Fatal(err)
		}
	}
	// A synthetic row of a different entity_type, to exercise the filter.
	if err := s.withControlTx(ctx, func(tx *sql.Tx) error {
		return appendChangelogTx(ctx, tx, "other", "o-1", "proj-1", "upsert", []byte(`{}`))
	}); err != nil {
		t.Fatal(err)
	}

	all, err := s.ChangelogSince(ctx, 0, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Fatalf("all = %d, want 4", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].Seq <= all[i-1].Seq {
			t.Fatalf("not ascending at %d", i)
		}
	}

	if after := mustSince(t, s, all[0].Seq, "", 100); len(after) != 3 {
		t.Fatalf("after cursor = %d, want 3", len(after))
	}
	if lim := mustSince(t, s, 0, "", 1); len(lim) != 1 || lim[0].Seq != all[0].Seq {
		t.Fatalf("limit=1 failed: %+v", lim)
	}
	if ar := mustSince(t, s, 0, "action_request", 100); len(ar) != 3 {
		t.Fatalf("action_request filter = %d, want 3", len(ar))
	}
	if none := mustSince(t, s, 0, "nope", 100); len(none) != 0 {
		t.Fatalf("nope filter = %d, want 0", len(none))
	}
	if beyond := mustSince(t, s, 9999, "", 100); len(beyond) != 0 {
		t.Fatalf("beyond = %d, want 0", len(beyond))
	}
}

func mustSince(t *testing.T, s *Store, since int64, entityType string, limit int) []ChangelogEntry {
	t.Helper()
	got, err := s.ChangelogSince(ctx, since, entityType, limit)
	if err != nil {
		t.Fatalf("ChangelogSince(%d,%q,%d): %v", since, entityType, limit, err)
	}
	return got
}

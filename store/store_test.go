package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ctx is the test context used for every store call.
var ctx = context.Background()

// open is a test helper: a fresh Store rooted at t.TempDir(), closed on cleanup.
func open(t *testing.T, opts Options) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func hash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// --- Migrations -------------------------------------------------------------

func TestControlMigrations_FreshAndIdempotent(t *testing.T) {
	dir := t.TempDir()

	s1, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	// schema_migrations records the control migrations.
	gotMax := maxAppliedVersion(t, filepath.Join(dir, "control.db"))
	wantMax := controlMigrations[len(controlMigrations)-1].version
	if gotMax != wantMax {
		t.Fatalf("control schema_migrations max = %d, want %d", gotMax, wantMax)
	}
	// Schema is usable (tenants table exists).
	if err := s1.UpsertTenant(ctx, "p1", hash("tok"), "active"); err != nil {
		t.Fatalf("upsert after fresh migrate: %v", err)
	}
	_ = s1.Close()

	// Re-open is idempotent: no re-apply, data intact.
	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer func() { _ = s2.Close() }()
	if got := maxAppliedVersion(t, filepath.Join(dir, "control.db")); got != wantMax {
		t.Fatalf("after re-open max = %d, want %d", got, wantMax)
	}
	if _, found, err := s2.TenantByProjectID(ctx, "p1"); err != nil || !found {
		t.Fatalf("tenant lost across re-open: found=%v err=%v", found, err)
	}
}

func TestProjectMigrations_FreshAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.CreateProjectDB(ctx, "proj-a"); err != nil {
		t.Fatalf("CreateProjectDB: %v", err)
	}
	pdb := filepath.Join(dir, "projects", "proj-a.db")
	wantMax := projectMigrations[len(projectMigrations)-1].version
	if got := maxAppliedVersion(t, pdb); got != wantMax {
		t.Fatalf("project schema_migrations max = %d, want %d", got, wantMax)
	}
	// CreateProjectDB again is idempotent.
	if err := s.CreateProjectDB(ctx, "proj-a"); err != nil {
		t.Fatalf("CreateProjectDB (idempotent): %v", err)
	}
	if got := maxAppliedVersion(t, pdb); got != wantMax {
		t.Fatalf("after re-create max = %d, want %d", got, wantMax)
	}
}

// TestSchemaVersionGuard_RefuseOnNewer writes a higher version row than the
// binary knows, then re-opens with the real (smaller) migration set and expects
// a clear refuse-on-newer error and NO corruption.
func TestSchemaVersionGuard_RefuseOnNewer(t *testing.T) {
	dir := t.TempDir()

	// Bootstrap a real control.db.
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.UpsertTenant(ctx, "p1", hash("tok"), "active"); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	_ = s.Close()

	// Simulate a NEWER binary having migrated this DB to a far-future version.
	future := controlMigrations[len(controlMigrations)-1].version + 5
	writeAppliedVersion(t, filepath.Join(dir, "control.db"), future)

	// Re-open with the current (older) binary → must refuse, clearly.
	_, err = Open(dir, Options{})
	if err == nil {
		t.Fatal("expected refuse-on-newer error, got nil")
	}
	if !IsSchemaTooNew(err) {
		t.Fatalf("expected IsSchemaTooNew, got %v", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("control.db")) ||
		!bytes.Contains([]byte(err.Error()), []byte(fmt.Sprintf("v%d", future))) {
		t.Fatalf("error message not clear: %v", err)
	}

	// No corruption: a real (>= future) binary would still open it; prove the
	// seeded data and schema survived by reading directly.
	if got := maxAppliedVersion(t, filepath.Join(dir, "control.db")); got != future {
		t.Fatalf("on-disk version mutated: got %d want %d", got, future)
	}
	raw := openRaw(t, filepath.Join(dir, "control.db"))
	defer raw.Close()
	var ph string
	if err := raw.QueryRow(`SELECT token_hash FROM tenants WHERE project_id='p1'`).Scan(&ph); err != nil {
		t.Fatalf("tenant data not intact after refused open: %v", err)
	}
	if ph != hash("tok") {
		t.Fatalf("tenant data corrupted: %q", ph)
	}
}

// TestSchemaVersionGuard_PerProjectDB exercises the same guard on a per-project
// DB (it runs on every project open, not just control.db).
func TestSchemaVersionGuard_PerProjectDB(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.CreateProjectDB(ctx, "proj-a"); err != nil {
		t.Fatalf("CreateProjectDB: %v", err)
	}
	_ = s.Close()

	pdb := filepath.Join(dir, "projects", "proj-a.db")
	future := projectMigrations[len(projectMigrations)-1].version + 3
	writeAppliedVersion(t, pdb, future)

	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open (control fine): %v", err)
	}
	defer func() { _ = s2.Close() }()

	// Touching the project triggers open+migrate → guard fires with a clear msg.
	_, _, err = s2.CustomerGet(ctx, "proj-a", "anything")
	if !IsSchemaTooNew(err) {
		t.Fatalf("expected IsSchemaTooNew on per-project open, got %v", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("projects/proj-a.db")) {
		t.Fatalf("per-project guard message not clear: %v", err)
	}
}

// --- Tenants ----------------------------------------------------------------

func TestTenants_UpsertLookupDelete(t *testing.T) {
	s := open(t, Options{})

	th := hash("bearer-1")
	if err := s.UpsertTenant(ctx, "proj-1", th, ""); err != nil { // default status
		t.Fatalf("UpsertTenant: %v", err)
	}

	// Hit by token hash.
	pid, status, found, err := s.TenantByTokenHash(ctx, th)
	if err != nil || !found {
		t.Fatalf("TenantByTokenHash hit: found=%v err=%v", found, err)
	}
	if pid != "proj-1" || status != "active" {
		t.Fatalf("got pid=%q status=%q, want proj-1/active", pid, status)
	}

	// Miss.
	if _, _, found, err := s.TenantByTokenHash(ctx, hash("nope")); err != nil || found {
		t.Fatalf("TenantByTokenHash miss: found=%v err=%v", found, err)
	}

	// Update status via upsert.
	if err := s.UpsertTenant(ctx, "proj-1", th, "suspended"); err != nil {
		t.Fatalf("UpsertTenant update: %v", err)
	}
	if _, status, _, _ := s.TenantByTokenHash(ctx, th); status != "suspended" {
		t.Fatalf("status not updated: %q", status)
	}

	// By project id.
	tn, found, err := s.TenantByProjectID(ctx, "proj-1")
	if err != nil || !found {
		t.Fatalf("TenantByProjectID: found=%v err=%v", found, err)
	}
	if tn.TokenHash != th || tn.Status != "suspended" {
		t.Fatalf("tenant row wrong: %+v", tn)
	}

	// Delete.
	if err := s.DeleteTenant(ctx, "proj-1"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	if _, found, _ := s.TenantByProjectID(ctx, "proj-1"); found {
		t.Fatal("tenant still present after delete")
	}
	// Delete absent is a no-op.
	if err := s.DeleteTenant(ctx, "proj-1"); err != nil {
		t.Fatalf("DeleteTenant (absent): %v", err)
	}
}

func TestTenants_TokenHashCollision(t *testing.T) {
	s := open(t, Options{})
	shared := hash("shared")
	if err := s.UpsertTenant(ctx, "proj-a", shared, "active"); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	// Same token_hash mapped to a different project must be rejected.
	if err := s.UpsertTenant(ctx, "proj-b", shared, "active"); err != ErrTenantExists {
		t.Fatalf("expected ErrTenantExists, got %v", err)
	}
}

// --- Per-project KV ---------------------------------------------------------

func TestCustomerKV_RoundTripAndLargeValue(t *testing.T) {
	s := open(t, Options{})
	if err := s.CreateProjectDB(ctx, "proj-1"); err != nil {
		t.Fatalf("CreateProjectDB: %v", err)
	}

	// Small round-trip.
	small := []byte(`{"plugins":[]}`)
	if err := s.CustomerPut(ctx, "proj-1", "wordpress/plugins", "application/json", small); err != nil {
		t.Fatalf("CustomerPut small: %v", err)
	}
	e, found, err := s.CustomerGet(ctx, "proj-1", "wordpress/plugins")
	if err != nil || !found {
		t.Fatalf("CustomerGet: found=%v err=%v", found, err)
	}
	if !bytes.Equal(e.Value, small) {
		t.Fatalf("value mismatch: %q", e.Value)
	}
	if e.ContentType != "application/json" || e.Size != int64(len(small)) {
		t.Fatalf("metadata wrong: ct=%q size=%d", e.ContentType, e.Size)
	}
	if e.UpdatedAt == 0 {
		t.Fatal("updated_at not recorded")
	}

	// >512 KB value — prove there's no Consul-style size cap.
	big := bytes.Repeat([]byte("x"), 600*1024)
	if err := s.CustomerPut(ctx, "proj-1", "big", "application/octet-stream", big); err != nil {
		t.Fatalf("CustomerPut big: %v", err)
	}
	e, found, err = s.CustomerGet(ctx, "proj-1", "big")
	if err != nil || !found {
		t.Fatalf("CustomerGet big: found=%v err=%v", found, err)
	}
	if e.Size != int64(len(big)) || !bytes.Equal(e.Value, big) {
		t.Fatalf("big value not round-tripped: size=%d", e.Size)
	}

	// Overwrite (full replace).
	if err := s.CustomerPut(ctx, "proj-1", "big", "text/plain", []byte("now small")); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	e, _, _ = s.CustomerGet(ctx, "proj-1", "big")
	if string(e.Value) != "now small" || e.ContentType != "text/plain" || e.Size != 9 {
		t.Fatalf("overwrite wrong: %+v", e)
	}

	// Delete.
	if err := s.CustomerDelete(ctx, "proj-1", "big"); err != nil {
		t.Fatalf("CustomerDelete: %v", err)
	}
	if _, found, _ := s.CustomerGet(ctx, "proj-1", "big"); found {
		t.Fatal("value present after delete")
	}
	// Get miss is not an error.
	if _, found, err := s.CustomerGet(ctx, "proj-1", "absent"); err != nil || found {
		t.Fatalf("miss: found=%v err=%v", found, err)
	}
}

// TestKVAreasAreSeparate proves managed_kv and customer_kv are independent
// tables: a write to one is invisible to the other (defense in depth).
func TestKVAreasAreSeparate(t *testing.T) {
	s := open(t, Options{})
	if err := s.CreateProjectDB(ctx, "proj-1"); err != nil {
		t.Fatalf("CreateProjectDB: %v", err)
	}
	const path = "metadata"
	if err := s.ManagedPut(ctx, "proj-1", path, "application/json", []byte(`managed`)); err != nil {
		t.Fatalf("ManagedPut: %v", err)
	}
	if err := s.CustomerPut(ctx, "proj-1", path, "application/json", []byte(`customer`)); err != nil {
		t.Fatalf("CustomerPut: %v", err)
	}

	m, _, _ := s.ManagedGet(ctx, "proj-1", path)
	c, _, _ := s.CustomerGet(ctx, "proj-1", path)
	if string(m.Value) != "managed" || string(c.Value) != "customer" {
		t.Fatalf("areas not separate: managed=%q customer=%q", m.Value, c.Value)
	}

	// Deleting from one area leaves the other intact.
	if err := s.CustomerDelete(ctx, "proj-1", path); err != nil {
		t.Fatalf("CustomerDelete: %v", err)
	}
	if _, found, _ := s.ManagedGet(ctx, "proj-1", path); !found {
		t.Fatal("managed value gone after customer delete")
	}
}

func TestCreateAndDeleteProjectDB_Files(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.CreateProjectDB(ctx, "proj-x"); err != nil {
		t.Fatalf("CreateProjectDB: %v", err)
	}
	base := filepath.Join(dir, "projects", "proj-x.db")
	if _, err := os.Stat(base); err != nil {
		t.Fatalf(".db not created: %v", err)
	}
	// Force WAL/SHM into existence with a write.
	if err := s.CustomerPut(ctx, "proj-x", "k", "", []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	if err := s.DeleteProjectDB(ctx, "proj-x"); err != nil {
		t.Fatalf("DeleteProjectDB: %v", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if _, err := os.Stat(base + suffix); !os.IsNotExist(err) {
			t.Fatalf("file %q still present after delete (err=%v)", base+suffix, err)
		}
	}
	// Delete absent project is a no-op.
	if err := s.DeleteProjectDB(ctx, "proj-x"); err != nil {
		t.Fatalf("DeleteProjectDB (absent): %v", err)
	}
}

func TestInvalidProjectID(t *testing.T) {
	s := open(t, Options{})
	for _, bad := range []string{"", ".", "..", "../escape", "a/b", `a\b`, "x..y", ".hidden", "-flag"} {
		err := s.CreateProjectDB(ctx, bad)
		if err == nil {
			t.Fatalf("expected error for project id %q", bad)
		}
		// The error must be the exported sentinel so the HTTP layer can map it
		// to a 400 (client error) rather than a 500.
		if !errors.Is(err, ErrInvalidProjectID) {
			t.Fatalf("project id %q: err = %v, want ErrInvalidProjectID", bad, err)
		}
	}
	// The same sentinel must surface from the KV data path too (it validates the
	// id before touching any file).
	if err := s.CustomerPut(ctx, "../escape", "k", "", []byte("v")); !errors.Is(err, ErrInvalidProjectID) {
		t.Fatalf("CustomerPut bad id: err = %v, want ErrInvalidProjectID", err)
	}
}

func TestInvalidPath(t *testing.T) {
	s := open(t, Options{})
	if err := s.CreateProjectDB(ctx, "proj"); err != nil {
		t.Fatalf("CreateProjectDB: %v", err)
	}
	// An empty path on a put is a client error → the exported sentinel.
	if err := s.CustomerPut(ctx, "proj", "", "", []byte("v")); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("CustomerPut empty path: err = %v, want ErrInvalidPath", err)
	}
	if err := s.ManagedPut(ctx, "proj", "", "", []byte("v")); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("ManagedPut empty path: err = %v, want ErrInvalidPath", err)
	}
}

func TestDoubleClose(t *testing.T) {
	s, err := Open(t.TempDir(), Options{ProjectIdleTimeout: time.Hour}) // sweeper running
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second Close must not panic (close of a closed channel) and must be a no-op.
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// --- LRU pool ---------------------------------------------------------------

// TestLRU_CapAndEviction opens more projects than the cap and asserts the open
// handle count stays bounded, the evicted handle is actually closed, and a
// reopen of the evicted project still works.
func TestLRU_CapAndEviction(t *testing.T) {
	const cap = 3
	s := open(t, Options{MaxOpenProjectDBs: cap})

	// Open cap+ projects, writing to each so the file is real. Each call leases
	// then releases, so nothing is pinned across iterations → the soft cap holds
	// exactly.
	const n = 8
	for i := 0; i < n; i++ {
		pid := fmt.Sprintf("p%02d", i)
		if err := s.CustomerPut(ctx, pid, "k", "", []byte("v")); err != nil {
			t.Fatalf("put %s: %v", pid, err)
		}
		if got := s.pool.openCount(); got > cap {
			t.Fatalf("open handles %d exceeded cap %d after %s", got, cap, pid)
		}
	}
	if got := s.pool.openCount(); got != cap {
		t.Fatalf("final open count = %d, want %d", got, cap)
	}

	// p00 was evicted long ago; prove its data survived eviction (the close
	// flushed cleanly) by reopening and reading it.
	e, found, err := s.CustomerGet(ctx, "p00", "k")
	if err != nil || !found || string(e.Value) != "v" {
		t.Fatalf("evicted project reopen failed: found=%v err=%v val=%q", found, err, e.Value)
	}

	// Reopening p00 should have evicted something else to stay at cap.
	if got := s.pool.openCount(); got != cap {
		t.Fatalf("after reopen, open count = %d, want %d", got, cap)
	}
}

// TestLRU_EvictedUnpinnedHandleClosed proves an UNPINNED evicted *sql.DB is
// closed (not leaked): acquire+release p1, then open p2 at cap=1, and the old p1
// handle is unusable.
func TestLRU_EvictedUnpinnedHandleClosed(t *testing.T) {
	s := open(t, Options{MaxOpenProjectDBs: 1})

	// Acquire p1 and capture its handle, then release it (unpinned).
	if err := s.CreateProjectDB(ctx, "p1"); err != nil {
		t.Fatalf("create p1: %v", err)
	}
	h1, release, err := s.pool.acquire("p1")
	if err != nil {
		t.Fatalf("acquire p1: %v", err)
	}
	if err := h1.PingContext(ctx); err != nil {
		t.Fatalf("p1 handle should be live: %v", err)
	}
	release() // now unpinned and evictable

	// Opening p2 with cap=1 evicts (and closes) the unpinned p1 handle.
	if err := s.CreateProjectDB(ctx, "p2"); err != nil {
		t.Fatalf("create p2: %v", err)
	}
	if err := h1.PingContext(ctx); err == nil {
		t.Fatal("expected evicted (unpinned) p1 handle to be closed (Ping should error)")
	}
}

// TestLRU_PinnedHandleNotClosed_C1 is the C1 use-after-evict regression. It pins
// p1's handle (acquire WITHOUT releasing), forces an eviction at cap=1 by
// opening p2, then proves the still-leased p1 handle is NOT closed and a query on
// it succeeds — the pool exceeded the cap rather than yanking a live handle.
func TestLRU_PinnedHandleNotClosed_C1(t *testing.T) {
	s := open(t, Options{MaxOpenProjectDBs: 1})

	if err := s.CreateProjectDB(ctx, "p1"); err != nil {
		t.Fatalf("create p1: %v", err)
	}
	if err := s.CustomerPut(ctx, "p1", "k", "", []byte("v")); err != nil {
		t.Fatalf("seed p1: %v", err)
	}

	// Pin p1 (do not release yet).
	h1, release, err := s.pool.acquire("p1")
	if err != nil {
		t.Fatalf("acquire p1: %v", err)
	}

	// Force the eviction pass with cap=1 by opening a second project. Under the
	// buggy code this would close p1's handle; with the lease it must not.
	if err := s.CreateProjectDB(ctx, "p2"); err != nil {
		t.Fatalf("create p2: %v", err)
	}

	// The pinned handle must still be usable mid-lease.
	var got string
	if err := h1.QueryRowContext(ctx, `SELECT value FROM customer_kv WHERE path='k'`).Scan(&got); err != nil {
		t.Fatalf("pinned handle closed out from under in-flight query (C1): %v", err)
	}
	if got != "v" {
		t.Fatalf("pinned handle read wrong value: %q", got)
	}

	// Releasing returns us to the soft cap on the next eviction opportunity.
	release()
	if _, _, err := s.CustomerGet(ctx, "p3", "k"); err != nil {
		t.Fatalf("post-release acquire: %v", err)
	}
	if got := s.pool.openCount(); got > 1 {
		t.Fatalf("after release, open count = %d, want <= 1", got)
	}
}

func TestLRU_IdleClose(t *testing.T) {
	s := open(t, Options{MaxOpenProjectDBs: 8, ProjectIdleTimeout: 50 * time.Millisecond})
	if err := s.CreateProjectDB(ctx, "idle-1"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := s.pool.openCount(); got != 1 {
		t.Fatalf("open count = %d, want 1", got)
	}
	// Wait for the idle sweep to close it.
	deadline := time.Now().Add(2 * time.Second)
	for s.pool.openCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("idle handle not closed within deadline (open=%d)", s.pool.openCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Still usable after idle close (reopens on demand).
	if _, _, err := s.CustomerGet(ctx, "idle-1", "k"); err != nil {
		t.Fatalf("reopen after idle close: %v", err)
	}
}

// TestConcurrentAccess hammers the pool from many goroutines across more
// projects than the cap to shake out lifecycle races (run with -race).
func TestConcurrentAccess(t *testing.T) {
	s := open(t, Options{MaxOpenProjectDBs: 4})

	const projects = 12
	const workers = 24
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				pid := fmt.Sprintf("c%02d", (w+i)%projects)
				key := fmt.Sprintf("k%d", i)
				val := []byte(fmt.Sprintf("w%d-i%d", w, i))
				if err := s.CustomerPut(ctx, pid, key, "", val); err != nil {
					errCh <- fmt.Errorf("put %s/%s: %w", pid, key, err)
					return
				}
				if _, _, err := s.CustomerGet(ctx, pid, key); err != nil {
					errCh <- fmt.Errorf("get %s/%s: %w", pid, key, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if got := s.pool.openCount(); got > 4 {
		t.Fatalf("open count %d exceeded cap under concurrency", got)
	}
}

// TestDeleteProjectDB_TombstoneBlocksResurrection is the deterministic M1
// regression. A lease is held open across the delete from a SEPARATE goroutine
// (a real delete request and a read request are always different goroutines), so
// DeleteProjectDB must:
//   - block while the lease is live (a query is in flight) rather than unlink
//     files under an open handle — the pinned handle stays usable until release;
//   - refuse a concurrent acquire with ErrProjectDeleting while the tombstone is
//     up (can't reopen + recreate the file mid-delete);
//   - once the lease releases, close the handle, unlink the files, and return —
//     leaving NO .db file behind (no resurrection through the drop→rm gap).
func TestDeleteProjectDB_TombstoneBlocksResurrection(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{MaxOpenProjectDBs: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.CustomerPut(ctx, "p1", "k", "", []byte("v")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Pin p1's handle, then hand the lease to a goroutine that holds it briefly
	// while we issue the delete, proving the handle stays usable mid-lease.
	h1, release, err := s.pool.acquire("p1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	leaseDone := make(chan struct{})
	go func() {
		defer close(leaseDone)
		// While the lease is held, the handle must remain live even though a
		// delete is racing it.
		time.Sleep(50 * time.Millisecond)
		if err := h1.PingContext(ctx); err != nil {
			t.Errorf("pinned handle closed before release during delete: %v", err)
		}
		release()
	}()

	// While the delete is blocked draining the lease, a concurrent acquire must
	// be refused with ErrProjectDeleting (tombstone up). Fire it slightly after
	// the delete starts.
	go func() {
		time.Sleep(10 * time.Millisecond)
		if _, _, err := s.CustomerGet(ctx, "p1", "k"); !errors.Is(err, ErrProjectDeleting) {
			// May also race to AFTER delete returns (new lifecycle) → nil err is
			// then fine; only an unexpected error type is a failure.
			if err != nil {
				t.Errorf("concurrent get during delete: want ErrProjectDeleting or nil, got %v", err)
			}
		}
	}()

	if err := s.DeleteProjectDB(ctx, "p1"); err != nil {
		t.Fatalf("DeleteProjectDB: %v", err)
	}
	<-leaseDone // ensure the lease goroutine's assertions ran

	// After delete returned: the OLD file was removed and not resurrected.
	base := filepath.Join(dir, "projects", "p1.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if _, err := os.Stat(base + suffix); !os.IsNotExist(err) {
			t.Fatalf("file %q present after delete (resurrected?): err=%v", base+suffix, err)
		}
	}
}

// TestConcurrentDeleteAndGet_M1 stress-races DeleteProjectDB against concurrent
// Get/Put. The guarantee under test: a racing access never panics or returns a
// corruption error — at worst it gets ErrProjectDeleting (lost the race to the
// tombstone). A Put that wins the race AFTER delete returns legitimately
// recreates the project (new lifecycle), so the file's presence afterward is
// NOT asserted here — that invariant is covered deterministically above.
func TestConcurrentDeleteAndGet_M1(t *testing.T) {
	s := open(t, Options{MaxOpenProjectDBs: 4})

	const iterations = 200
	for i := 0; i < iterations; i++ {
		pid := fmt.Sprintf("d%03d", i)
		if err := s.CustomerPut(ctx, pid, "k", "", []byte("v")); err != nil {
			t.Fatalf("seed %s: %v", pid, err)
		}

		start := make(chan struct{}) // release barrier: maximize overlap
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := s.DeleteProjectDB(ctx, pid); err != nil {
				t.Errorf("DeleteProjectDB %s: %v", pid, err)
			}
		}()
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, _, err := s.CustomerGet(ctx, pid, "k"); err != nil &&
					!errors.Is(err, ErrProjectDeleting) {
					t.Errorf("Get %s during delete: unexpected err %v", pid, err)
				}
				if err := s.CustomerPut(ctx, pid, "k2", "", []byte("v2")); err != nil &&
					!errors.Is(err, ErrProjectDeleting) {
					t.Errorf("Put %s during delete: unexpected err %v", pid, err)
				}
			}()
		}
		close(start)
		wg.Wait()
	}
}

// --- raw-DB test helpers ----------------------------------------------------

// openRaw opens a SQLite file directly (bypassing the store) for assertions.
func openRaw(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("openRaw %s: %v", path, err)
	}
	return db
}

func maxAppliedVersion(t *testing.T, path string) int {
	t.Helper()
	db := openRaw(t, path)
	defer db.Close()
	var v sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatalf("read max version from %s: %v", path, err)
	}
	return int(v.Int64)
}

func writeAppliedVersion(t *testing.T, path string, version int) {
	t.Helper()
	db := openRaw(t, path)
	defer db.Close()
	if _, err := db.Exec(
		`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`,
		version, time.Now().Unix(),
	); err != nil {
		t.Fatalf("write version %d to %s: %v", version, path, err)
	}
}

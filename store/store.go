// Package store is the agent's embedded SQLite data plane: one control.db per
// node (tenant/auth map) plus one DB per project (customer + platform metadata
// KV). It replaces the node's use of Consul as a KV store. It is self-contained
// — the HTTP layer consumes this API; this package wires no server and nothing
// into main.go.
//
// Layout under dataDir:
//
//	<dataDir>/control.db                 control plane (tenants, schema_migrations)
//	<dataDir>/projects/<project_id>.db   one per project (meta, managed_kv, customer_kv)
//
// Every DB opens with WAL + synchronous=FULL (authoritative data: a returned
// COMMIT is on disk) + busy_timeout + foreign_keys=ON, and runs its own
// migrations on open with a refuse-on-newer schema-version guard so a
// rolled-back binary never corrupts a newer on-disk schema.
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DefaultMaxOpenProjectDBs caps how many per-project DB handles the LRU pool
// keeps open at once. Density is ~100–150 projects/node but only the active
// write set needs an open handle, so a few dozen covers steady state; the rest
// open on demand. Configurable via Options.MaxOpenProjectDBs.
const DefaultMaxOpenProjectDBs = 64

// Options configures a Store. The zero value is valid and uses defaults.
type Options struct {
	// MaxOpenProjectDBs caps simultaneously-open per-project handles
	// (<=0 → DefaultMaxOpenProjectDBs). The LRU pool evicts (closing) the
	// least-recently-used handle past the cap.
	MaxOpenProjectDBs int

	// ProjectIdleTimeout closes per-project handles unused for this long
	// (0 → never idle-close; eviction is then driven only by the cap).
	ProjectIdleTimeout time.Duration
}

// Store is the node's embedded data plane: the open control.db plus an LRU pool
// of per-project DB handles. Methods are safe for concurrent use. Construct with
// Open; release with Close.
type Store struct {
	dataDir string
	control *sql.DB
	pool    *connPool
}

// Open opens (creating + migrating as needed) control.db under dataDir and
// prepares the per-project LRU pool. Per-project DBs are opened lazily on first
// use. Returns a schema-too-new error (IsSchemaTooNew) — without corrupting
// anything — if control.db was written by a newer binary.
func Open(dataDir string, opts Options) (*Store, error) {
	if dataDir == "" {
		return nil, errors.New("store: Open requires a non-empty data dir")
	}
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("store: create data dir %q: %w", dataDir, err)
	}

	controlSQL, err := openSQLite(filepath.Join(dataDir, "control.db"), true)
	if err != nil {
		return nil, err
	}
	if err := runMigrations(controlSQL, "control.db", controlMigrations); err != nil {
		_ = controlSQL.Close()
		return nil, err
	}

	maxOpen := opts.MaxOpenProjectDBs
	if maxOpen <= 0 {
		maxOpen = DefaultMaxOpenProjectDBs
	}

	pool := newConnPool(
		filepath.Join(dataDir, "projects"),
		maxOpen,
		opts.ProjectIdleTimeout,
		func(db *sql.DB, name string) error { return runMigrations(db, name, projectMigrations) },
	)

	return &Store{
		dataDir: dataDir,
		control: controlSQL,
		pool:    pool,
	}, nil
}

// Close closes the per-project pool (and its idle sweeper) and control.db.
// Idempotent: a second call is a safe no-op. The Store must not be used after
// the first Close.
func (s *Store) Close() error {
	var firstErr error
	if err := s.pool.close(); err != nil {
		firstErr = err
	}
	if err := s.control.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// validateProjectID rejects ids that would escape the projects dir or otherwise
// be unsafe as a filename. project_id is a controller-provisioned stable id, but
// since it becomes a path component we still guard it (defense in depth — a
// crafted id must never traverse the filesystem). Also rejects "." / ".." and a
// leading "." or "-" (hidden files; ids an arg parser would read as a flag).
func validateProjectID(projectID string) error {
	if projectID == "" {
		return fmt.Errorf("%w: empty", ErrInvalidProjectID)
	}
	if projectID != filepath.Base(projectID) ||
		strings.ContainsAny(projectID, `/\`) ||
		strings.Contains(projectID, "..") ||
		strings.ContainsRune(projectID, 0) ||
		projectID == "." ||
		strings.HasPrefix(projectID, ".") ||
		strings.HasPrefix(projectID, "-") {
		return fmt.Errorf("%w %q", ErrInvalidProjectID, projectID)
	}
	return nil
}

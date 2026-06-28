package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// projectMigrations is the ordered migration list for every per-project DB
// (projects/<project_id>.db). ADDITIVE-BY-DEFAULT: append
// new migrations only. Each per-project DB runs these on first open (the LRU
// pool migrates on open), so the same rollback-tolerance rules as control.db
// apply — an older binary meeting a newer per-project schema refuses via
// the schema-version guard rather than corrupting a customer's metadata DB.
var projectMigrations = []migration{
	{
		version: 1,
		up: func(tx *sql.Tx) error {
			// Two distinct KV tables, not one — defense in depth:
			//   managed_kv  : PLATFORM writes (published env details), customer
			//                 reads only. Reconstructable.
			//   customer_kv : CUSTOMER writes & reads (the /db/ space).
			//                 Authoritative → the DR target.
			// The customer write path touches ONLY customer_kv, so a routing bug
			// structurally cannot write into the read-only area.
			_, err := tx.Exec(`
				CREATE TABLE meta (
					key   TEXT PRIMARY KEY,
					value TEXT
				);
				CREATE TABLE managed_kv (
					path         TEXT PRIMARY KEY,
					value        BLOB NOT NULL,
					content_type TEXT,
					size         INTEGER NOT NULL,
					updated_at   INTEGER NOT NULL
				);
				CREATE TABLE customer_kv (
					path         TEXT PRIMARY KEY,
					value        BLOB NOT NULL,
					content_type TEXT,
					size         INTEGER NOT NULL,
					updated_at   INTEGER NOT NULL
				);
			`)
			return err
		},
	},
}

// KVEntry is a value read out of a per-project KV table.
type KVEntry struct {
	Path        string
	Value       []byte
	ContentType string
	Size        int64
	UpdatedAt   int64
}

// kvArea names a per-project KV table. Keeping the table name behind a typed
// constant (rather than threading a string) means the customer path can only
// ever name customerKV — the read-only guarantee for managed_kv is structural.
type kvArea string

const (
	customerKV kvArea = "customer_kv"
	managedKV  kvArea = "managed_kv"
)

// CreateProjectDB creates (and migrates) the per-project DB for projectID,
// returning once the file exists and its schema is current. Idempotent: calling
// it for an existing project just opens + verifies it. The handle is left in the
// LRU pool warm for the imminent first request.
func (s *Store) CreateProjectDB(ctx context.Context, projectID string) error {
	if err := validateProjectID(projectID); err != nil {
		return err
	}
	_, release, err := s.pool.acquire(projectID)
	if err != nil {
		return fmt.Errorf("store: create project db %q: %w", projectID, err)
	}
	release()
	return nil
}

// DeleteProjectDB closes any open handle for projectID and removes its DB files
// (.db, -wal, -shm). Removing an absent project is a no-op. The tenant mapping
// is removed separately via DeleteTenant.
func (s *Store) DeleteProjectDB(ctx context.Context, projectID string) error {
	if err := validateProjectID(projectID); err != nil {
		return err
	}
	if err := s.pool.remove(projectID); err != nil {
		return fmt.Errorf("store: delete project db %q: %w", projectID, err)
	}
	return nil
}

// CustomerGet reads a value from the customer-writable area (the /db/ space).
// found=false on a miss (not an error).
func (s *Store) CustomerGet(ctx context.Context, projectID, path string) (e KVEntry, found bool, err error) {
	return s.kvGet(ctx, projectID, customerKV, path)
}

// CustomerPut writes value to the customer-writable area, recording size +
// content_type + updated_at. Full-value replace (upsert). There is no size cap —
// killing the Consul 512 KB ceiling is a goal.
func (s *Store) CustomerPut(ctx context.Context, projectID, path, contentType string, value []byte) error {
	return s.kvPut(ctx, projectID, customerKV, path, contentType, value)
}

// CustomerDelete removes a value from the customer-writable area. Deleting an
// absent path is a no-op (no error).
func (s *Store) CustomerDelete(ctx context.Context, projectID, path string) error {
	return s.kvDelete(ctx, projectID, customerKV, path)
}

// ManagedGet reads a value from the platform-managed (read-only to the customer)
// area. found=false on a miss (not an error).
func (s *Store) ManagedGet(ctx context.Context, projectID, path string) (e KVEntry, found bool, err error) {
	return s.kvGet(ctx, projectID, managedKV, path)
}

// ManagedPut writes a value to the platform-managed area. Only the internal /
// trusted path calls this — never the customer Bearer endpoint. Full-value
// replace (upsert); records size + content_type + updated_at.
func (s *Store) ManagedPut(ctx context.Context, projectID, path, contentType string, value []byte) error {
	return s.kvPut(ctx, projectID, managedKV, path, contentType, value)
}

// ManagedDelete removes a value from the platform-managed area. Deleting an
// absent path is a no-op (no error).
func (s *Store) ManagedDelete(ctx context.Context, projectID, path string) error {
	return s.kvDelete(ctx, projectID, managedKV, path)
}

func (s *Store) kvGet(ctx context.Context, projectID string, area kvArea, path string) (KVEntry, bool, error) {
	if err := validateProjectID(projectID); err != nil {
		return KVEntry{}, false, err
	}
	db, release, err := s.pool.acquire(projectID)
	if err != nil {
		return KVEntry{}, false, err
	}
	defer release()

	e := KVEntry{Path: path}
	// area is a package-internal typed constant (never user input), so this is
	// not a SQL-injection surface.
	q := fmt.Sprintf(
		`SELECT value, content_type, size, updated_at FROM %s WHERE path = ?`, area,
	)
	var ct sql.NullString
	switch err := db.QueryRowContext(ctx, q, path).Scan(&e.Value, &ct, &e.Size, &e.UpdatedAt); {
	case errors.Is(err, sql.ErrNoRows):
		return KVEntry{}, false, nil
	case err != nil:
		return KVEntry{}, false, fmt.Errorf("store: %s get %q/%q: %w", area, projectID, path, err)
	}
	e.ContentType = ct.String
	return e, true, nil
}

func (s *Store) kvPut(ctx context.Context, projectID string, area kvArea, path, contentType string, value []byte) error {
	if err := validateProjectID(projectID); err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("%w: kv put requires a non-empty path", ErrInvalidPath)
	}
	db, release, err := s.pool.acquire(projectID)
	if err != nil {
		return err
	}
	defer release()

	q := fmt.Sprintf(`
		INSERT INTO %s (path, value, content_type, size, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			value        = excluded.value,
			content_type = excluded.content_type,
			size         = excluded.size,
			updated_at   = excluded.updated_at
	`, area)
	if _, err := db.ExecContext(ctx, q, path, value, nullable(contentType), len(value), time.Now().Unix()); err != nil {
		return fmt.Errorf("store: %s put %q/%q: %w", area, projectID, path, err)
	}
	return nil
}

func (s *Store) kvDelete(ctx context.Context, projectID string, area kvArea, path string) error {
	if err := validateProjectID(projectID); err != nil {
		return err
	}
	db, release, err := s.pool.acquire(projectID)
	if err != nil {
		return err
	}
	defer release()

	q := fmt.Sprintf(`DELETE FROM %s WHERE path = ?`, area)
	if _, err := db.ExecContext(ctx, q, path); err != nil {
		return fmt.Errorf("store: %s delete %q/%q: %w", area, projectID, path, err)
	}
	return nil
}

// nullable maps "" to a SQL NULL so an unset content_type stays NULL rather than
// an empty string.
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

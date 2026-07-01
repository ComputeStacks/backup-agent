package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// migration is one ordered, forward schema change. version must be strictly
// increasing across the slice; up runs inside its own transaction.
//
// Migration convention — ADDITIVE BY DEFAULT:
// because binaries are rollback-installable from the apt repo, an OLDER binary
// can be asked to open a DB a NEWER binary migrated. To keep that safe, every
// migration should only ADD nullable columns / new tables / new indexes that
// older code can ignore — never rename, drop, repurpose, or NOT-NULL-without-
// default an existing column. A change that can't be expressed additively needs
// a paired down-migration and careful sequencing; until then, keep it additive.
// The schema-version guard below is the backstop for the rollback case: an old
// binary that meets a newer schema refuses to open rather than corrupting it.
type migration struct {
	version int
	up      func(*sql.Tx) error
}

// schemaVersionError is returned when the on-disk schema is NEWER than the
// running binary understands (a downgrade/rollback hazard). We refuse to open
// rather than risk operating on — and corrupting — a schema we don't know.
type schemaVersionError struct {
	dbName    string // human label, e.g. "control.db" or "projects/<id>.db"
	onDisk    int    // highest applied version found on disk
	supported int    // highest version this binary knows
}

func (e *schemaVersionError) Error() string {
	return fmt.Sprintf(
		"%s is schema v%d; this binary supports v%d — reinstall a newer cs-agent (>= the one that wrote it) or restore a pre-upgrade backup",
		e.dbName, e.onDisk, e.supported,
	)
}

// IsSchemaTooNew reports whether err is the refuse-on-newer guard firing. Lets
// callers (and tests) distinguish "binary is too old for this DB" from any other
// open failure.
func IsSchemaTooNew(err error) bool {
	var sve *schemaVersionError
	return errors.As(err, &sve)
}

// runMigrations brings db up to date against the ordered migrations slice.
//
//   - ensures schema_migrations exists,
//   - reads the set of already-applied versions,
//   - REFUSES (schemaVersionError) if the max applied version exceeds the
//     highest version this binary knows — never crashes or corrupts,
//   - applies each pending migration in its own transaction and records it.
//
// dbName is only used for error messages. Migrations must be ordered by version
// ascending with no duplicates.
func runMigrations(db *sql.DB, dbName string, migrations []migration) error {
	if _, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER PRIMARY KEY,
			applied_at INTEGER NOT NULL
		)`,
	); err != nil {
		return fmt.Errorf("%s: create schema_migrations: %w", dbName, err)
	}

	applied, maxApplied, err := appliedVersions(db)
	if err != nil {
		return fmt.Errorf("%s: read schema_migrations: %w", dbName, err)
	}

	// Highest version this binary knows. Migrations are ordered, so it's the last.
	supported := 0
	if n := len(migrations); n > 0 {
		supported = migrations[n-1].version
	}

	// Schema-version guard: on-disk schema newer than we understand → refuse.
	if maxApplied > supported {
		return &schemaVersionError{dbName: dbName, onDisk: maxApplied, supported: supported}
	}

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		if err := applyMigration(db, m); err != nil {
			return fmt.Errorf("%s: apply migration v%d: %w", dbName, m.version, err)
		}
	}
	return nil
}

// appliedVersions returns the set of applied versions and the max of them (0 if
// none applied).
func appliedVersions(db *sql.DB) (map[int]bool, int, error) {
	rows, err := db.Query(`SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	applied := make(map[int]bool)
	maxApplied := 0
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, 0, err
		}
		applied[v] = true
		if v > maxApplied {
			maxApplied = v
		}
	}
	return applied, maxApplied, rows.Err()
}

// applyMigration runs one migration's up func and records its version in the
// same transaction, so a crash mid-migration leaves it un-recorded and it
// re-runs (migrations must therefore be idempotent-safe to retry from scratch).
func applyMigration(db *sql.DB, m migration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // no-op after a successful Commit

	if err := m.up(tx); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`,
		m.version, time.Now().Unix(),
	); err != nil {
		return err
	}
	return tx.Commit()
}

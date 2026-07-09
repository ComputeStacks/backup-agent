package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// ChangelogEntry is one row of the append-only, node-scoped changelog: the
// replication spine a controller polls to project node-owned state. Payload is
// the full JSON snapshot of the entity on an upsert, nil on a delete. It is
// json.RawMessage (not []byte) so it inlines as JSON in responses rather than
// base64-encoding.
type ChangelogEntry struct {
	Seq        int64           `json:"seq"`
	EntityType string          `json:"entity_type"`
	EntityID   string          `json:"entity_id"`
	ProjectID  string          `json:"project_id,omitempty"`
	Op         string          `json:"op"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	CreatedAt  int64           `json:"created_at"`
}

// withControlTx runs fn inside a single control.db transaction. It is the
// runtime analog of applyMigration and the ONLY non-migration transaction the
// store opens. Begin / deferred-Rollback / Commit — the Rollback is a no-op
// after a successful Commit.
//
// control.db opens IMMEDIATE (see openSQLite), so BeginTx takes the write lock
// at BEGIN. Combined with SQLite WAL single-writer serialization, no two
// control.db write transactions overlap: a changelog seq is allocated and
// committed under one held write lock, so allocation order == commit order. The
// seq is therefore a monotonic, commit-ordered replication cursor — a poller
// reading "seq > cursor ORDER BY seq" never skips a committed row. (Contiguity
// is not guaranteed nor required: a rolled-back tx or future pruning may leave
// harmless gaps, since the cursor only ever moves past committed seqs.)
func (s *Store) withControlTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // no-op after a successful Commit

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// appendChangelogTx inserts one changelog row inside an existing transaction.
// Taking a *sql.Tx (not the *sql.DB) structurally forces every append to share
// its entity mutation's transaction, so a changelog row exists IFF that mutation
// committed. payload is the full JSON snapshot on an upsert, nil on a delete; it
// is bound as TEXT (string), NOT []byte — a []byte bind would store BLOB storage
// class in the TEXT column and json1 (json_extract, …) would then treat it as
// binary JSONB rather than JSON text. createdAt is passed in (not read from the
// clock here) so the changelog row shares its entity row's timestamp exactly.
func appendChangelogTx(ctx context.Context, tx *sql.Tx, entityType, entityID, projectID, op string, payload []byte, createdAt int64) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO changelog (entity_type, entity_id, project_id, op, payload, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, entityType, entityID, nullable(projectID), op, nullableJSON(payload), createdAt); err != nil {
		return fmt.Errorf("store: append changelog %s/%s: %w", entityType, entityID, err)
	}
	return nil
}

// nullableJSON maps an empty JSON payload to SQL NULL and otherwise binds the
// bytes as a string, so a TEXT-affinity column stores TEXT storage class (not
// BLOB). The []byte analog of nullable.
func nullableJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

// defaultChangelogPull bounds a ChangelogSince call whose caller passes a
// non-positive limit (SQLite treats a negative LIMIT as unbounded — a full-table
// scan). The HTTP handler validates limit separately; this is defense in depth.
const defaultChangelogPull = 100

// ChangelogSince returns changelog rows with seq > since, ordered by seq
// ascending, capped at limit. A non-positive limit is clamped to
// defaultChangelogPull. If entityType is non-empty only that type is returned.
// This is the controller's pull channel: it tracks a per-node cursor and asks
// for "changes since" it.
func (s *Store) ChangelogSince(ctx context.Context, since int64, entityType string, limit int) ([]ChangelogEntry, error) {
	if limit <= 0 {
		limit = defaultChangelogPull
	}
	var (
		rows *sql.Rows
		err  error
	)
	if entityType == "" {
		rows, err = s.control.QueryContext(ctx, `
			SELECT seq, entity_type, entity_id, project_id, op, payload, created_at
			  FROM changelog WHERE seq > ? ORDER BY seq LIMIT ?`, since, limit)
	} else {
		rows, err = s.control.QueryContext(ctx, `
			SELECT seq, entity_type, entity_id, project_id, op, payload, created_at
			  FROM changelog WHERE seq > ? AND entity_type = ? ORDER BY seq LIMIT ?`, since, entityType, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("store: changelog since %d: %w", since, err)
	}
	defer rows.Close()

	var out []ChangelogEntry
	for rows.Next() {
		var (
			e       ChangelogEntry
			projID  sql.NullString
			payload sql.NullString
		)
		if err := rows.Scan(&e.Seq, &e.EntityType, &e.EntityID, &projID, &e.Op, &payload, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: scan changelog row: %w", err)
		}
		e.ProjectID = projID.String
		if payload.Valid {
			e.Payload = json.RawMessage(payload.String)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate changelog: %w", err)
	}
	return out, nil
}

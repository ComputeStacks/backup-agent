package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Task is a unit of work for this node — a backup/restore/delete/export or a
// firewall reload. It replaces the Consul jobs/<jid> envelope. The controller
// submits a task DOWN (POST /v1/admin/tasks) and the agent reports lifecycle UP
// via the changelog (entity_type "task"). name is one of volume.backup,
// volume.restore, backup.delete, backup.export, firewall. Result carries the
// terminal payload (export url/size/expiry/error; a backup's last_backup).
type Task struct {
	ID        string          `json:"id"`
	ProjectID string          `json:"project_id,omitempty"`
	Name      string          `json:"name"`
	Node      string          `json:"node"`
	Volume    string          `json:"volume,omitempty"`
	Archive   string          `json:"archive,omitempty"`
	AuditID   int64           `json:"audit_id,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
	Status    string          `json:"status"`
	Result    json.RawMessage `json:"result,omitempty"`
	CreatedAt int64           `json:"created_at"`
	UpdatedAt int64           `json:"updated_at"`
}

// Task status values.
const (
	TaskPending   = "pending"
	TaskRunning   = "running"
	TaskCompleted = "completed"
	TaskFailed    = "failed"
	TaskCancelled = "cancelled"
)

const taskColumns = `id, project_id, name, node, volume, archive, audit_id, params, status, result_json, created_at, updated_at`

// scanTask reads a task row from either *sql.Row or *sql.Rows.
func scanTask(row interface{ Scan(...any) error }) (Task, error) {
	var (
		t       Task
		projID  sql.NullString
		volume  sql.NullString
		archive sql.NullString
		auditID sql.NullInt64
		params  sql.NullString
		result  sql.NullString
	)
	if err := row.Scan(&t.ID, &projID, &t.Name, &t.Node, &volume, &archive, &auditID, &params, &t.Status, &result, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return Task{}, err
	}
	t.ProjectID = projID.String
	t.Volume = volume.String
	t.Archive = archive.String
	t.AuditID = auditID.Int64
	if params.Valid {
		t.Params = json.RawMessage(params.String)
	}
	if result.Valid {
		t.Result = json.RawMessage(result.String)
	}
	return t, nil
}

// getTaskTx reads a task within an existing transaction (for building the
// post-mutation changelog snapshot).
func getTaskTx(ctx context.Context, tx *sql.Tx, id string) (Task, error) {
	return scanTask(tx.QueryRowContext(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = ?`, id))
}

// CreateTask inserts a task and, in the SAME control.db transaction, appends its
// changelog row (entity_type "task", op "upsert"). It is idempotent on the task
// id: a duplicate POST (same id) is a no-op — no second row, no second changelog
// entry, no re-dispatch — and returns created=false. This is what makes
// at-least-once controller delivery safe. Dispatch to the worker pool is the
// dispatcher's job (v2.3.0), not this method's.
func (s *Store) CreateTask(ctx context.Context, t Task) (created bool, err error) {
	if t.ID == "" || t.Name == "" || t.Node == "" {
		return false, errors.New("store: CreateTask requires id, name, node")
	}
	now := time.Now().Unix()
	if t.Status == "" {
		t.Status = TaskPending
	}
	t.CreatedAt = now
	t.UpdatedAt = now
	snapshot, err := json.Marshal(t)
	if err != nil {
		return false, fmt.Errorf("store: marshal task %q: %w", t.ID, err)
	}

	err = s.withControlTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			INSERT INTO tasks (id, project_id, name, node, volume, archive, audit_id, params, status, result_json, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO NOTHING
		`, t.ID, nullable(t.ProjectID), t.Name, t.Node, nullable(t.Volume), nullable(t.Archive), t.AuditID, nullableJSON(t.Params), t.Status, nullableJSON(t.Result), t.CreatedAt, t.UpdatedAt)
		if err != nil {
			return fmt.Errorf("store: insert task %q: %w", t.ID, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: task %q rows affected: %w", t.ID, err)
		}
		if n == 0 {
			return nil // duplicate id: no row inserted -> no changelog append
		}
		created = true
		return appendChangelogTx(ctx, tx, "task", t.ID, t.ProjectID, "upsert", snapshot, t.CreatedAt)
	})
	if err != nil {
		return false, err // the insert/append rolled back: never report created on error
	}
	return created, nil
}

// UpdateTaskStatus transitions a task's status and, if result is non-nil,
// overwrites result_json (a nil result leaves the existing value). It re-reads
// the row inside the tx and appends the full updated snapshot to the changelog
// (op "upsert"). Updating an absent task returns sql.ErrNoRows.
func (s *Store) UpdateTaskStatus(ctx context.Context, id, status string, result json.RawMessage) error {
	if id == "" || status == "" {
		return errors.New("store: UpdateTaskStatus requires id and status")
	}
	now := time.Now().Unix()
	return s.withControlTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE tasks SET status = ?, result_json = COALESCE(?, result_json), updated_at = ?
			WHERE id = ?
		`, status, nullableJSON(result), now, id)
		if err != nil {
			return fmt.Errorf("store: update task %q: %w", id, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: task %q rows affected: %w", id, err)
		}
		if n == 0 {
			return fmt.Errorf("store: update task %q: %w", id, sql.ErrNoRows)
		}
		t, err := getTaskTx(ctx, tx, id)
		if err != nil {
			return fmt.Errorf("store: reload task %q: %w", id, err)
		}
		snapshot, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("store: marshal task %q: %w", id, err)
		}
		return appendChangelogTx(ctx, tx, "task", t.ID, t.ProjectID, "upsert", snapshot, now)
	})
}

// CancelPendingTask flips a task pending -> cancelled (and appends the snapshot)
// only while it is still pending; a task already dispatched/terminal is left
// untouched. Returns cancelled=false if the task was not pending (or absent).
func (s *Store) CancelPendingTask(ctx context.Context, id string) (cancelled bool, err error) {
	if id == "" {
		return false, errors.New("store: CancelPendingTask requires id")
	}
	now := time.Now().Unix()
	err = s.withControlTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE tasks SET status = ?, updated_at = ? WHERE id = ? AND status = ?
		`, TaskCancelled, now, id, TaskPending)
		if err != nil {
			return fmt.Errorf("store: cancel task %q: %w", id, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: task %q rows affected: %w", id, err)
		}
		if n == 0 {
			return nil // not pending (dispatched/terminal/absent): no-op, not changelogged
		}
		cancelled = true
		t, err := getTaskTx(ctx, tx, id)
		if err != nil {
			return fmt.Errorf("store: reload task %q: %w", id, err)
		}
		snapshot, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("store: marshal task %q: %w", id, err)
		}
		return appendChangelogTx(ctx, tx, "task", t.ID, t.ProjectID, "upsert", snapshot, now)
	})
	if err != nil {
		return false, err // the update/append rolled back: never report cancelled on error
	}
	return cancelled, nil
}

// GetTask returns a task by id. found=false on a miss (not an error).
func (s *Store) GetTask(ctx context.Context, id string) (Task, bool, error) {
	t, err := scanTask(s.control.QueryRowContext(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = ?`, id))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Task{}, false, nil
	case err != nil:
		return Task{}, false, fmt.Errorf("store: get task %q: %w", id, err)
	default:
		return t, true, nil
	}
}

// ListPendingTasks returns this node's pending tasks in creation order. It is
// the dispatcher's boot/backstop drain source (v2.3.0).
func (s *Store) ListPendingTasks(ctx context.Context, node string) ([]Task, error) {
	rows, err := s.control.QueryContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE node = ? AND status = ? ORDER BY created_at, id`,
		node, TaskPending)
	if err != nil {
		return nil, fmt.Errorf("store: list pending tasks: %w", err)
	}
	defer rows.Close()

	var out []Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan task row: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate tasks: %w", err)
	}
	return out, nil
}

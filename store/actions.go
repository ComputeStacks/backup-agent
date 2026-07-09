package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ActionRequest is an outbox row: a container-requested action awaiting the
// controller's pull. The agent is generic — it never interprets action_type or
// params. Params is opaque JSON (may be nil).
type ActionRequest struct {
	ID         string          `json:"id"`
	ProjectID  string          `json:"project_id"`
	ActionType string          `json:"action_type"`
	Params     json.RawMessage `json:"params,omitempty"`
	Status     string          `json:"status"`
	CreatedAt  int64           `json:"created_at"`
	UpdatedAt  int64           `json:"updated_at"`
}

// CreateActionRequest inserts an outbox row and, in the SAME control.db
// transaction, appends its changelog row (entity_type "action_request", op
// "upsert", payload = the full row snapshot). Atomicity means the controller can
// never observe an action missing from the outbox, nor miss one that is in it.
// Generic by design: it does not interpret actionType. id is caller-generated
// (a uuid); params is opaque JSON (nil for none).
func (s *Store) CreateActionRequest(ctx context.Context, id, projectID, actionType string, params json.RawMessage) (ActionRequest, error) {
	if id == "" || projectID == "" || actionType == "" {
		return ActionRequest{}, errors.New("store: CreateActionRequest requires id, project_id, action_type")
	}
	now := time.Now().Unix()
	ar := ActionRequest{
		ID:         id,
		ProjectID:  projectID,
		ActionType: actionType,
		Params:     params,
		Status:     "pending",
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	snapshot, err := json.Marshal(ar) // the full-row snapshot the changelog carries
	if err != nil {
		return ActionRequest{}, fmt.Errorf("store: marshal action_request %q: %w", id, err)
	}

	err = s.withControlTx(ctx, func(tx *sql.Tx) error {
		// Write-first (see withControlTx): the outbox row, then its changelog
		// row, both under the single write lock so the seq stays gap-free.
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO action_requests (id, project_id, action_type, params, status, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`, ar.ID, ar.ProjectID, ar.ActionType, nullableJSON(ar.Params), ar.Status, ar.CreatedAt, ar.UpdatedAt); err != nil {
			return fmt.Errorf("store: insert action_request %q: %w", id, err)
		}
		return appendChangelogTx(ctx, tx, "action_request", ar.ID, ar.ProjectID, "upsert", snapshot)
	})
	if err != nil {
		return ActionRequest{}, err
	}
	return ar, nil
}

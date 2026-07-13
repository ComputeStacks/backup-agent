package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Volume is a volume's desired-state, submitted DOWN by the controller
// (replacing the Consul volumes/<name> key). Config is the opaque full
// types.Volume JSON (it carries freq, backup, retention, hooks, …); the agent
// indexes name/project_id/node out of it for lookups. name is the docker volume
// name (node-unique). The changelog entity_type is "volume".
//
// Note: last_backup remains inside Config for now but the agent never reads it;
// backup freshness flows UP via a completed backup task's result, keeping this a
// pure DOWN table (see the plan §4).
type Volume struct {
	Name      string          `json:"name"`
	ProjectID string          `json:"project_id,omitempty"`
	Node      string          `json:"node"`
	Config    json.RawMessage `json:"config"`
	UpdatedAt int64           `json:"updated_at"`
}

const volumeColumns = `name, project_id, node, config, updated_at`

func scanVolume(row interface{ Scan(...any) error }) (Volume, error) {
	var (
		v      Volume
		projID sql.NullString
		config sql.NullString
	)
	if err := row.Scan(&v.Name, &projID, &v.Node, &config, &v.UpdatedAt); err != nil {
		return Volume{}, err
	}
	v.ProjectID = projID.String
	if config.Valid {
		v.Config = json.RawMessage(config.String)
	}
	return v, nil
}

// PutVolume upserts a volume's desired-state, appends its changelog row
// (entity_type "volume", op "upsert"), and latches the volumes-populated
// sentinel — all in one transaction. The sentinel tells the DOWN readers
// (scheduler, compact/prune) that an empty control.db means "unpopulated, leave
// alone" no longer applies.
func (s *Store) PutVolume(ctx context.Context, v Volume) error {
	if v.Name == "" || v.Node == "" || len(v.Config) == 0 {
		return errors.New("store: PutVolume requires name, node, config")
	}
	now := time.Now().Unix()
	v.UpdatedAt = now
	snapshot, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("store: marshal volume %q: %w", v.Name, err)
	}

	return s.withControlTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO volumes (name, project_id, node, config, updated_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(name) DO UPDATE SET
				project_id = excluded.project_id,
				node       = excluded.node,
				config     = excluded.config,
				updated_at = excluded.updated_at
		`, v.Name, nullable(v.ProjectID), v.Node, nullableJSON(v.Config), v.UpdatedAt); err != nil {
			return fmt.Errorf("store: upsert volume %q: %w", v.Name, err)
		}
		if err := setMetaTx(ctx, tx, MetaVolumesPopulated, "1"); err != nil {
			return err
		}
		return appendChangelogTx(ctx, tx, "volume", v.Name, v.ProjectID, "upsert", snapshot, now)
	})
}

// GetVolume returns a volume's desired-state by name. found=false on a miss.
func (s *Store) GetVolume(ctx context.Context, name string) (Volume, bool, error) {
	v, err := scanVolume(s.control.QueryRowContext(ctx, `SELECT `+volumeColumns+` FROM volumes WHERE name = ?`, name))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Volume{}, false, nil
	case err != nil:
		return Volume{}, false, fmt.Errorf("store: get volume %q: %w", name, err)
	default:
		return v, true, nil
	}
}

// DeleteVolume removes a volume's desired-state and appends a delete changelog
// row (nil payload). Deleting an absent volume is a no-op (no error, no
// changelog). The volumes-populated sentinel stays latched.
func (s *Store) DeleteVolume(ctx context.Context, name, projectID string) error {
	if name == "" {
		return errors.New("store: DeleteVolume requires name")
	}
	now := time.Now().Unix()
	return s.withControlTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM volumes WHERE name = ?`, name)
		if err != nil {
			return fmt.Errorf("store: delete volume %q: %w", name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: volume %q rows affected: %w", name, err)
		}
		if n == 0 {
			return nil // absent: no-op, not changelogged
		}
		return appendChangelogTx(ctx, tx, "volume", name, projectID, "delete", nil, now)
	})
}

// ListVolumesByNode returns all volumes whose node matches. It is the
// storeScheduleSource / compact / prune volume list (replacing consul.Keys).
func (s *Store) ListVolumesByNode(ctx context.Context, node string) ([]Volume, error) {
	rows, err := s.control.QueryContext(ctx, `SELECT `+volumeColumns+` FROM volumes WHERE node = ? ORDER BY name`, node)
	if err != nil {
		return nil, fmt.Errorf("store: list volumes by node: %w", err)
	}
	defer rows.Close()

	var out []Volume
	for rows.Next() {
		v, err := scanVolume(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan volume row: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate volumes: %w", err)
	}
	return out, nil
}

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Repository is a borg repository's observed state, reported UP by the agent
// (replacing the Consul borg/repository/<name> key): its on-disk/total size and
// archive-name list. name matches the volume name. The changelog entity_type is
// "repository". This is standing observed-state, not a unit of work — hence its
// own table rather than folding into tasks.
type Repository struct {
	Name       string   `json:"name"`
	SizeOnDisk int64    `json:"size_on_disk"`
	TotalSize  int64    `json:"total_size"`
	Archives   []string `json:"archives"`
	UpdatedAt  int64    `json:"updated_at"`
}

// UpsertRepository writes a repository's observed state and appends its
// changelog row (entity_type "repository", op "upsert") in one transaction. This
// is the store-backed successor to borg.Repository.SyncConsul.
func (s *Store) UpsertRepository(ctx context.Context, r Repository) error {
	if r.Name == "" {
		return errors.New("store: UpsertRepository requires name")
	}
	now := time.Now().Unix()
	r.UpdatedAt = now
	if r.Archives == nil {
		r.Archives = []string{}
	}
	archives, err := json.Marshal(r.Archives)
	if err != nil {
		return fmt.Errorf("store: marshal repository archives %q: %w", r.Name, err)
	}
	snapshot, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("store: marshal repository %q: %w", r.Name, err)
	}

	return s.withControlTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO repositories (name, size_on_disk, total_size, archives, updated_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(name) DO UPDATE SET
				size_on_disk = excluded.size_on_disk,
				total_size   = excluded.total_size,
				archives     = excluded.archives,
				updated_at   = excluded.updated_at
		`, r.Name, r.SizeOnDisk, r.TotalSize, nullableJSON(archives), r.UpdatedAt); err != nil {
			return fmt.Errorf("store: upsert repository %q: %w", r.Name, err)
		}
		return appendChangelogTx(ctx, tx, "repository", r.Name, "", "upsert", snapshot, now)
	})
}

// GetRepository returns a repository's observed state by name. found=false on a
// miss.
func (s *Store) GetRepository(ctx context.Context, name string) (Repository, bool, error) {
	var (
		r          Repository
		sizeOnDisk sql.NullInt64
		totalSize  sql.NullInt64
		archives   sql.NullString
	)
	err := s.control.QueryRowContext(ctx,
		`SELECT name, size_on_disk, total_size, archives, updated_at FROM repositories WHERE name = ?`, name).
		Scan(&r.Name, &sizeOnDisk, &totalSize, &archives, &r.UpdatedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Repository{}, false, nil
	case err != nil:
		return Repository{}, false, fmt.Errorf("store: get repository %q: %w", name, err)
	default:
		r.SizeOnDisk = sizeOnDisk.Int64 // nullable columns: absent -> 0
		r.TotalSize = totalSize.Int64
		if archives.Valid && archives.String != "" {
			if err := json.Unmarshal([]byte(archives.String), &r.Archives); err != nil {
				return Repository{}, false, fmt.Errorf("store: unmarshal repository archives %q: %w", name, err)
			}
		}
		return r, true, nil
	}
}

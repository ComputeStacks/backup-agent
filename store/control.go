package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// controlMigrations is the ordered migration list for control.db (one per node).
// See brainstorm §D.1. ADDITIVE-BY-DEFAULT: append new migrations, never edit a
// shipped one (an already-applied version is skipped, so edits to it never run).
var controlMigrations = []migration{
	{
		version: 1,
		up: func(tx *sql.Tx) error {
			// tenant/auth map: replaces the Consul ACL token. Provisioned by the
			// controller, used by the customer endpoint offline. token_hash is
			// sha256(Bearer) — never store plaintext.
			_, err := tx.Exec(`
				CREATE TABLE tenants (
					project_id TEXT PRIMARY KEY,
					token_hash TEXT NOT NULL,
					status     TEXT NOT NULL DEFAULT 'active',
					created_at INTEGER NOT NULL,
					updated_at INTEGER NOT NULL
				);
				CREATE UNIQUE INDEX idx_tenants_token ON tenants(token_hash);
			`)
			return err
		},
	},
}

// ErrTenantExists is returned when UpsertTenant would collide on token_hash with
// a DIFFERENT project (the token_hash unique index). A re-upsert of the same
// project's row is fine.
var ErrTenantExists = errors.New("store: token_hash already mapped to a different project")

// Tenant is the local token→project mapping the customer endpoint authenticates
// against (offline-capable; see brainstorm §A).
type Tenant struct {
	ProjectID string
	TokenHash string
	Status    string
	CreatedAt int64
	UpdatedAt int64
}

// UpsertTenant inserts or updates the tenant row for projectID. status defaults
// to "active" when empty. It is the controller-provision / re-provision path.
//
// token_hash carries a UNIQUE index: pointing a token already mapped to project
// A at a different project B returns ErrTenantExists rather than silently
// stealing the mapping.
func (s *Store) UpsertTenant(ctx context.Context, projectID, tokenHash, status string) error {
	if projectID == "" || tokenHash == "" {
		return errors.New("store: UpsertTenant requires project_id and token_hash")
	}
	if status == "" {
		status = "active"
	}
	now := time.Now().Unix()

	_, err := s.control.ExecContext(ctx, `
		INSERT INTO tenants (project_id, token_hash, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(project_id) DO UPDATE SET
			token_hash = excluded.token_hash,
			status     = excluded.status,
			updated_at = excluded.updated_at
	`, projectID, tokenHash, status, now, now)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrTenantExists
		}
		return fmt.Errorf("store: upsert tenant %q: %w", projectID, err)
	}
	return nil
}

// TenantByTokenHash resolves a hashed Bearer token to its project. This is the
// hot customer-auth path: hash the presented Bearer, look it up here, scope all
// further work to the returned project. found=false on a miss (not an error).
func (s *Store) TenantByTokenHash(ctx context.Context, tokenHash string) (projectID, status string, found bool, err error) {
	row := s.control.QueryRowContext(ctx,
		`SELECT project_id, status FROM tenants WHERE token_hash = ?`, tokenHash,
	)
	switch err := row.Scan(&projectID, &status); {
	case errors.Is(err, sql.ErrNoRows):
		return "", "", false, nil
	case err != nil:
		return "", "", false, fmt.Errorf("store: tenant by token: %w", err)
	default:
		return projectID, status, true, nil
	}
}

// TenantByProjectID returns the full tenant row for a project. found=false on a
// miss (not an error).
func (s *Store) TenantByProjectID(ctx context.Context, projectID string) (t Tenant, found bool, err error) {
	row := s.control.QueryRowContext(ctx,
		`SELECT project_id, token_hash, status, created_at, updated_at
		   FROM tenants WHERE project_id = ?`, projectID,
	)
	switch err := row.Scan(&t.ProjectID, &t.TokenHash, &t.Status, &t.CreatedAt, &t.UpdatedAt); {
	case errors.Is(err, sql.ErrNoRows):
		return Tenant{}, false, nil
	case err != nil:
		return Tenant{}, false, fmt.Errorf("store: tenant by project %q: %w", projectID, err)
	default:
		return t, true, nil
	}
}

// DeleteTenant removes the tenant mapping for projectID. Deleting an absent
// project is a no-op (no error). This only removes the auth mapping; the
// project's per-project DB is removed separately via DeleteProjectDB.
func (s *Store) DeleteTenant(ctx context.Context, projectID string) error {
	if _, err := s.control.ExecContext(ctx, `DELETE FROM tenants WHERE project_id = ?`, projectID); err != nil {
		return fmt.Errorf("store: delete tenant %q: %w", projectID, err)
	}
	return nil
}

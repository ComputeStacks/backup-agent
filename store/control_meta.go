package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// control_meta is a small key/value table in control.db for node-local
// janitor/consumer state that is NOT node truth and therefore is never appended
// to the changelog. It mirrors the per-project meta table shape.
//
// Today it holds the per-domain "populated" sentinels; later it also holds the
// changelog acked watermark (v2.6.0).
const (
	// MetaFirewallPopulated / MetaVolumesPopulated latch true the first time the
	// controller submits that domain's desired-state DOWN. Until the latch is
	// set, a DOWN reader must NOT converge kernel/schedule state to "empty" from
	// an unpopulated control.db (an unpopulated table is indistinguishable from
	// "controller sent zero" and would otherwise close all ports / drop every
	// backup schedule). This is the guard behind the coordinated DOWN cutovers.
	MetaFirewallPopulated = "firewall_populated"
	MetaVolumesPopulated  = "volumes_populated"
)

// GetMeta returns the value for key. found=false on a miss (not an error).
func (s *Store) GetMeta(ctx context.Context, key string) (value string, found bool, err error) {
	row := s.control.QueryRowContext(ctx, `SELECT value FROM control_meta WHERE key = ?`, key)
	switch err := row.Scan(&value); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("store: get control_meta %q: %w", key, err)
	default:
		return value, true, nil
	}
}

// SetMeta upserts key=value. This is consumer/janitor state, not node truth, so
// it is a plain autocommit write and never appends a changelog row.
func (s *Store) SetMeta(ctx context.Context, key, value string) error {
	if _, err := s.control.ExecContext(ctx, `
		INSERT INTO control_meta (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, key, value); err != nil {
		return fmt.Errorf("store: set control_meta %q: %w", key, err)
	}
	return nil
}

// IsPopulated reports whether the given domain's populated sentinel is latched.
// domain is one of MetaFirewallPopulated / MetaVolumesPopulated.
func (s *Store) IsPopulated(ctx context.Context, domain string) (bool, error) {
	v, found, err := s.GetMeta(ctx, domain)
	if err != nil {
		return false, err
	}
	return found && v == "1", nil
}

// setMetaTx upserts a control_meta row inside an existing transaction, so a
// populated sentinel latches atomically with the desired-state write that earns
// it. Not changelogged (see the package comment).
func setMetaTx(ctx context.Context, tx *sql.Tx, key, value string) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO control_meta (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, key, value); err != nil {
		return fmt.Errorf("store: set control_meta %q: %w", key, err)
	}
	return nil
}

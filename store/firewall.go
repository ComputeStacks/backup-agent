package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// FirewallRules is a node's published-port NAT desired-state, submitted DOWN by
// the controller (replacing the Consul nodes/<host>/ingress_rules key). Rules is
// the opaque firewall.NatRules JSON; the nftables renderer parses it unchanged.
// The changelog entity_type is "firewall_rule", keyed by node.
type FirewallRules struct {
	Node      string          `json:"node"`
	Rules     json.RawMessage `json:"rules"`
	UpdatedAt int64           `json:"updated_at"`
}

// PutFirewallRules upserts a node's rule set, appends its changelog row
// (entity_type "firewall_rule", op "upsert"), and latches the firewall-populated
// sentinel — all in one transaction. An explicit empty rule set (controller sent
// zero) is a legitimate PUT and still latches the sentinel, so the renderer can
// then safely converge to "no published ports" — distinct from the unpopulated
// case where it must leave the kernel untouched.
func (s *Store) PutFirewallRules(ctx context.Context, node string, rules json.RawMessage) error {
	if node == "" || len(rules) == 0 {
		return errors.New("store: PutFirewallRules requires node and rules")
	}
	now := time.Now().Unix()
	fr := FirewallRules{Node: node, Rules: rules, UpdatedAt: now}
	snapshot, err := json.Marshal(fr)
	if err != nil {
		return fmt.Errorf("store: marshal firewall_rules %q: %w", node, err)
	}

	return s.withControlTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO firewall_rules (node, rules, updated_at)
			VALUES (?, ?, ?)
			ON CONFLICT(node) DO UPDATE SET
				rules      = excluded.rules,
				updated_at = excluded.updated_at
		`, node, nullableJSON(rules), now); err != nil {
			return fmt.Errorf("store: upsert firewall_rules %q: %w", node, err)
		}
		if err := setMetaTx(ctx, tx, MetaFirewallPopulated, "1"); err != nil {
			return err
		}
		return appendChangelogTx(ctx, tx, "firewall_rule", node, "", "upsert", snapshot, now)
	})
}

// GetFirewallRules returns a node's rule set. found=false on a miss.
func (s *Store) GetFirewallRules(ctx context.Context, node string) (FirewallRules, bool, error) {
	var (
		fr    FirewallRules
		rules sql.NullString
	)
	err := s.control.QueryRowContext(ctx, `SELECT node, rules, updated_at FROM firewall_rules WHERE node = ?`, node).
		Scan(&fr.Node, &rules, &fr.UpdatedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return FirewallRules{}, false, nil
	case err != nil:
		return FirewallRules{}, false, fmt.Errorf("store: get firewall_rules %q: %w", node, err)
	default:
		if rules.Valid {
			fr.Rules = json.RawMessage(rules.String)
		}
		return fr, true, nil
	}
}

// DeleteFirewallRules removes a node's rule set and appends a delete changelog
// row. Deleting an absent node is a no-op. The firewall-populated sentinel stays
// latched (use an explicit empty PUT to mean "zero rules").
func (s *Store) DeleteFirewallRules(ctx context.Context, node string) error {
	if node == "" {
		return errors.New("store: DeleteFirewallRules requires node")
	}
	now := time.Now().Unix()
	return s.withControlTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM firewall_rules WHERE node = ?`, node)
		if err != nil {
			return fmt.Errorf("store: delete firewall_rules %q: %w", node, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: firewall_rules %q rows affected: %w", node, err)
		}
		if n == 0 {
			return nil // absent: no-op, not changelogged
		}
		return appendChangelogTx(ctx, tx, "firewall_rule", node, "", "delete", nil, now)
	})
}

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
		// Enforce a single firewall row per node-DB: drop any row stored under a
		// different label (e.g. after a hostname/label change) so the projection
		// can't go stale and GetFirewallRules is unambiguous. The common same-label
		// re-PUT finds no others and just upserts (no delete churn).
		stale, err := otherFirewallNodes(ctx, tx, node)
		if err != nil {
			return err
		}
		if len(stale) > 0 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM firewall_rules WHERE node <> ?`, node); err != nil {
				return fmt.Errorf("store: clear stale firewall_rules: %w", err)
			}
			for _, n := range stale {
				if err := appendChangelogTx(ctx, tx, "firewall_rule", n, "", "delete", nil, now); err != nil {
					return err
				}
			}
		}
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

// otherFirewallNodes returns the node labels of every firewall_rules row except
// keep (used to enforce the single-row invariant on PUT/DELETE).
func otherFirewallNodes(ctx context.Context, tx *sql.Tx, keep string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT node FROM firewall_rules WHERE node <> ?`, keep)
	if err != nil {
		return nil, fmt.Errorf("store: list firewall_rules labels: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("store: scan firewall_rules label: %w", err)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// GetFirewallRules returns this node's rule set. found=false on a miss. There is
// no node argument: firewall_rules is a singleton per node-DB (the controller PUTs
// only this node's rules to this node's endpoint), so we return the single/latest
// row regardless of the label the controller used.
func (s *Store) GetFirewallRules(ctx context.Context) (FirewallRules, bool, error) {
	var (
		fr    FirewallRules
		rules sql.NullString
	)
	err := s.control.QueryRowContext(ctx,
		`SELECT node, rules, updated_at FROM firewall_rules ORDER BY updated_at DESC, node LIMIT 1`).
		Scan(&fr.Node, &rules, &fr.UpdatedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return FirewallRules{}, false, nil
	case err != nil:
		return FirewallRules{}, false, fmt.Errorf("store: get firewall_rules: %w", err)
	default:
		if rules.Valid {
			fr.Rules = json.RawMessage(rules.String)
		}
		return fr, true, nil
	}
}

// DeleteFirewallRules clears this node's rule set (all rows — normally one),
// appending a delete changelog row per removed label so the controller's
// projection stays consistent. Deleting when absent is a no-op. The
// firewall-populated sentinel stays latched (use an explicit empty PUT to mean
// "zero rules").
func (s *Store) DeleteFirewallRules(ctx context.Context) error {
	now := time.Now().Unix()
	return s.withControlTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT node FROM firewall_rules`)
		if err != nil {
			return fmt.Errorf("store: list firewall_rules for delete: %w", err)
		}
		var nodes []string
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				rows.Close()
				return fmt.Errorf("store: scan firewall_rules node: %w", err)
			}
			nodes = append(nodes, n)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("store: iterate firewall_rules: %w", err)
		}
		rows.Close()
		if len(nodes) == 0 {
			return nil // absent: no-op, not changelogged
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM firewall_rules`); err != nil {
			return fmt.Errorf("store: delete firewall_rules: %w", err)
		}
		for _, n := range nodes {
			if err := appendChangelogTx(ctx, tx, "firewall_rule", n, "", "delete", nil, now); err != nil {
				return err
			}
		}
		return nil
	})
}

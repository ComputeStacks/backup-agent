package firewall

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"cs-agent/store"
)

// TestLoadExpectedRules covers the store-backed desired-state read that replaced
// the Consul ingress_rules read. (The full Reconcile sentinel gate — skip the
// render while unpopulated — also depends on ensureProjectIsolation's iptables
// shell-out and store.IsPopulated, so it is exercised end-to-end on the dev node;
// store.IsPopulated itself is unit-tested in the store package.)
func TestLoadExpectedRules(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	ctx := context.Background()
	hostname, _ := os.Hostname()

	// No firewall_rules row for this node -> (nil, nil): "no published ports".
	rules, err := loadExpectedRules(ctx, st)
	if err != nil {
		t.Fatalf("loadExpectedRules (missing): %v", err)
	}
	if rules != nil {
		t.Fatalf("expected nil rules for a missing row, got %+v", rules)
	}

	// A stored rule set parses into NatRules.
	if err := st.PutFirewallRules(ctx, hostname,
		json.RawMessage(`{"rules":[{"proto":"tcp","port":80,"nat":10080,"dest":"10.0.0.2"}]}`)); err != nil {
		t.Fatalf("put firewall rules: %v", err)
	}
	rules, err = loadExpectedRules(ctx, st)
	if err != nil {
		t.Fatalf("loadExpectedRules (present): %v", err)
	}
	if rules == nil || len(rules.Rules) != 1 || rules.Rules[0].Port != 80 || rules.Rules[0].Nat != 10080 {
		t.Fatalf("parsed rules = %+v", rules)
	}
}

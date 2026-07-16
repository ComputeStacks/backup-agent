package store

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestFirewallSingleton covers the node-scope redesign: firewall_rules is a
// singleton per node-DB. Get returns the latest row regardless of the label the
// controller used (so a relabeled PUT can't cause an empty-render outage), and
// Delete clears all rows.
func TestFirewallSingleton(t *testing.T) {
	s := open(t, Options{})

	if err := s.PutFirewallRules(ctx, "node-a", json.RawMessage(`{"rules":[{"proto":"tcp","port":80}]}`)); err != nil {
		t.Fatal(err)
	}
	// A PUT under a different node label (e.g. FQDN vs short name) — Get must still
	// return the latest, not miss on a hostname mismatch.
	if err := s.PutFirewallRules(ctx, "node-a.example.com", json.RawMessage(`{"rules":[{"proto":"tcp","port":443}]}`)); err != nil {
		t.Fatal(err)
	}
	fr, found, err := s.GetFirewallRules(ctx)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if !strings.Contains(string(fr.Rules), "443") {
		t.Fatalf("Get did not return the latest PUT: %s", fr.Rules)
	}

	if err := s.DeleteFirewallRules(ctx); err != nil {
		t.Fatal(err)
	}
	if got := countTable(t, s, "firewall_rules"); got != 0 {
		t.Fatalf("firewall_rules = %d after delete, want 0 (clears all)", got)
	}
	if _, found, _ := s.GetFirewallRules(ctx); found {
		t.Fatal("Get found rules after delete")
	}
}

package firewall

import (
	"context"
	"cs-agent/store"
	"encoding/json"

	"github.com/getsentry/sentry-go"
)

type NatRules struct {
	Rules []NatRule `json:"rules"`
}

type NatRule struct {
	Proto  string `json:"proto"`
	Nat    int    `json:"nat"`
	Port   int    `json:"port"`
	Dest   string `json:"dest"`
	Driver string `json:"driver"` // calico_docker bridge
}

// loadExpectedRules reads this node's published-port desired-state from control.db
// (the singleton firewall_rules row) into NatRules. It is the desired-state source
// for the native cs_agent nftables renderer (unchanged renderer, store source).
//
// Return contract (relied on by Reconcile):
//   - (rules, nil)  -- rules parsed successfully (may have zero entries).
//   - (nil,   nil)  -- no firewall_rules row: a legitimate "no published ports"
//     desired state. Reconcile renders an empty table.
//   - (nil,   err)  -- a load or parse error: Reconcile leaves kernel state
//     untouched and retries next reconcile.
func loadExpectedRules(ctx context.Context, st *store.Store) (rules *NatRules, err error) {
	fr, found, err := st.GetFirewallRules(ctx)
	if err != nil {
		sentry.CaptureException(err)
		csFirewallLog().Warn("Fatal error loading rules from store", "error", err.Error())
		return nil, err
	}

	if !found {
		csFirewallLog().Info("No ingress rules found")
		return nil, nil
	}

	if jsonErr := json.Unmarshal(fr.Rules, &rules); jsonErr != nil {
		sentry.CaptureException(jsonErr)
		csFirewallLog().Error("Error parsing response as json", "data", string(fr.Rules))
		// Surface the parse failure instead of returning a possibly-nil/partial
		// ruleset; the caller treats a non-nil err as "skip this reconcile".
		return nil, jsonErr
	}
	return rules, nil
}

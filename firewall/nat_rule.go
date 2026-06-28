package firewall

import (
	"encoding/json"
	"os"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
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

// loadExpectedRules reads this node's desired published-port state from Consul
// (nodes/<hostname>/ingress_rules) into NatRules. This is the desired-state
// source for the native cs_agent nftables renderer; it is unchanged by the
// nftables migration (the renderer was swapped, not the source).
//
// Return contract (relied on by Perform):
//   - (rules, nil)  -- rules parsed successfully (may have zero entries).
//   - (nil,   nil)  -- no ingress_rules key for this node: a legitimate "no
//     published ports" desired state. Perform renders an empty table.
//   - (nil,   err)  -- a load or parse error: Perform leaves kernel state
//     untouched and retries next reconcile.
func loadExpectedRules(consul *consulAPI.Client) (rules *NatRules, err error) {
	hostname, _ := os.Hostname()
	kv := consul.KV()
	opts := &consulAPI.QueryOptions{RequireConsistent: true}
	data, _, err := kv.Get("nodes/"+hostname+"/ingress_rules", opts)
	if err != nil {
		sentry.CaptureException(err)
		csFirewallLog().Warn("Fatal error loading rules from consul", "error", err.Error())
		return rules, err
	}

	if data == nil {
		csFirewallLog().Info("No ingress rules found")
		return rules, err
	}

	jsonErr := json.Unmarshal(data.Value, &rules)
	if jsonErr != nil {
		sentry.CaptureException(jsonErr)
		csFirewallLog().Error("Error parsing response as json", "data", string(data.Value))
		// Surface the parse failure instead of returning a possibly-nil/partial
		// ruleset; the caller treats a non-nil err as "skip this reconcile".
		return nil, jsonErr
	}
	return rules, err
}

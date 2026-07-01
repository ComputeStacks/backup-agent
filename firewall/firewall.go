package firewall

import (
	"cs-agent/log"
	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
)

func Perform(consul *consulAPI.Client) {
	defer sentry.Recover()
	// Enforce cross-project network isolation. This stays iptables-based in
	// DOCKER-USER (its correctness depends on ordering relative to Docker's own
	// chains) and is independent of the published-port reconcile below (which
	// owns the native cs_agent table), so the two never interfere.
	ensureProjectIsolation()
	// Gather rules from ComputeStacks
	expectedRules, err := loadExpectedRules(consul)
	if err != nil {
		// On a load error we leave the kernel state untouched (do not render an
		// empty table) -- a transient Consul error must not tear down live
		// published ports. The next reconcile re-renders from desired state.
		return
	}
	// loadExpectedRules returns a nil *NatRules when this node has no ingress
	// rules in consul. That is a legitimate desired state (no published ports),
	// so render an empty cs_agent table to converge -- closing any ports that
	// were previously open. buildPlan handles a nil ruleset (-> empty table).
	if err := renderTable(expectedRules); err != nil {
		sentry.CaptureException(err)
		csFirewallLog().Error("Failed to render cs_agent nftables table", "error", err.Error())
		return
	}
}

// Logger
func csFirewallLog() hclog.Logger {
	return log.New().Named("cs-firewall")
}

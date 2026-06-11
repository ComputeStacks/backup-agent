package firewall

import (
	"cs-agent/log"
	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
)

func Perform(consul *consulAPI.Client) {
	defer sentry.Recover()
	// Enforce cross-project network isolation. This lives in DOCKER-USER and is
	// independent of the per-port reconcile below (which only manages the
	// expose-ports / container-inbound chains), so the two never interfere.
	ensureProjectIsolation()
	// Gather rules from ComputeStacks
	expectedRules, err := loadExpectedRules(consul)
	if err != nil {
		return
	}
	// loadExpectedRules returns a nil *NatRules when this node has no ingress
	// rules in consul (or the payload failed to parse). There is nothing to
	// reconcile in that case, so skip rather than dereference nil below.
	if expectedRules == nil {
		return
	}
	// Gather current rules
	currentRules := hostIPTableRules()
	currentForwardRules := hostForwardIPTableRules()

	// Loop through current rules
	// 	* Check if rule exists in expected rules
	//		* Delete if rule is missing from expected rules
	for _, l := range currentRules {
		if !expectedRules.ruleExists(l) {
			deleteHostRule(l)
		}
	}
	for _, l := range currentForwardRules {
		if !expectedRules.forwardRuleExists(l) {
			deleteForwardHostRule(l)
		}
	}

	// Loop through expected rules
	//	* Check if rule exists
	//		* Add it if missing
	for _, r := range expectedRules.Rules {
		if !r.hostRuleExists(currentRules) {
			r.apply()
		}
		if !r.forwardHostRuleExists(currentForwardRules) {
			r.applyForwardRule()
		}
	}

}

// Logger
func csFirewallLog() hclog.Logger {
	return log.New().Named("cs-firewall")
}

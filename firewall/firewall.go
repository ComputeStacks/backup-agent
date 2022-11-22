package firewall

import (
	"cs-agent/log"
	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
)

func Perform(consul *consulAPI.Client) {
	defer sentry.Recover()
	// Gather rules from ComputeStacks
	expectedRules, err := loadExpectedRules(consul)
	if err != nil {
		return
	}
	// Gather current rules
	currentRules := hostIPTableRules()

	// Loop through current rules
	// 	* Check if rule exists in expected rules
	//		* Delete if rule is missing from expected rules
	for _, l := range currentRules {
		if !expectedRules.ruleExists(l) {
			deleteHostRule(l)
		}
	}

	// Loop through expected rules
	//	* Check if rule exists
	//		* Add it if missing
	for _, r := range expectedRules.Rules {
		if !r.hostRuleExists(currentRules) {
			r.apply()
		}
	}

}

// Logger
func csFirewallLog() hclog.Logger {
	return log.New().Named("cs-firewall")
}



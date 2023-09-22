package firewall

import (
	"fmt"
	"os/exec"
	"strconv"
)

func (r *NatRule) applyForwardRule() {
	if r.Driver == "calico-node" {
		return
	}

	execCmd := fmt.Sprintf("%s -A %s", iptablesCmd(), r.forwardRule())
	csFirewallLog().Info("Adding Forward Rule", "rule", execCmd)

	cmd := exec.Command("bash", "-c", execCmd)
	output, _ := cmd.CombinedOutput()
	if string(output) != "" {
		csFirewallLog().Debug("Add Forward Host Rule", "result", string(output))
	}
	return
}

func (r *NatRule) forwardRule() string {
	return "container-inbound -d " + r.Dest + "/32 -p " + r.Proto + " -m " + r.Proto + " --dport " + strconv.Itoa(r.Port) + " -j ACCEPT"
}

// Provide a list of expected rules for this node
func (r *NatRule) forwardHostRuleExists(existingRules []string) bool {
	for _, l := range existingRules {
		if "-A "+r.forwardRule() == l {
			return true
		}
	}
	return false
}

// Given a set of expected rules, determine if a rule should exist.
func (r *NatRules) forwardRuleExists(line string) bool {
	for _, rule := range r.Rules {
		if "-A "+rule.forwardRule() == line {
			return true
		}
	}
	return false
}

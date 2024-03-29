package firewall

import (
	"encoding/json"
	"fmt"
	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
	"os"
	"os/exec"
	"strconv"
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
	}
	return rules, err
}

func (r *NatRule) apply() {
	execCmd := fmt.Sprintf("%s -t nat -A %s", iptablesCmd(), r.iptableRule())
	csFirewallLog().Info("Adding NAT Rule", "rule", execCmd)
	cmd := exec.Command("bash", "-c", execCmd)

	output, _ := cmd.CombinedOutput()
	if string(output) != "" {
		csFirewallLog().Debug("Add Nat Host Rule", "result", string(output))
	}
	return
}

func (r *NatRule) iptableRule() string {
	return "expose-ports -p " + r.Proto + " -m " + r.Proto + " --dport " + strconv.Itoa(r.Nat) + " -j DNAT --to-destination " + r.Dest + ":" + strconv.Itoa(r.Port)
}

// Provide a list of expected rules for this node
func (r *NatRule) hostRuleExists(existingRules []string) bool {
	for _, l := range existingRules {
		if "-A "+r.iptableRule() == l {
			return true
		}
	}
	return false
}

// Given a set of expected rules, determine if a rule should exist.
func (r *NatRules) ruleExists(line string) bool {
	for _, rule := range r.Rules {
		if "-A "+rule.iptableRule() == line {
			return true
		}
	}
	return false
}

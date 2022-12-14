package firewall

import (
	"os/exec"
	"strings"
)

type IPTableRules struct {
	Lines []IPTableRule
}
type IPTableRule struct {
	Line string
}

func hostIPTableRules() (rules []string) {
	cmd := exec.Command("bash", "-c", "iptables-save | grep '\\-A expose-ports'")
	output, _ := cmd.CombinedOutput()
	if string(output) == "" {
		return []string{}
	}
	rawSplit := strings.Split(string(output), "\n")
	for _, v := range rawSplit {
		if strings.Contains(v, "expose-ports") {
			rules = append(rules, v)
		}
	}
	return rules
}


func deleteHostRule(line string) {
	l := strings.ReplaceAll(line, "-A", "-D")
	csFirewallLog().Info("Deleting Rule", "rule", l)
	cmd := exec.Command("bash", "-c",  "iptables -t nat " + l)
	output, _ := cmd.CombinedOutput()
	if string(output) == "" {
		return
	}
	return
}

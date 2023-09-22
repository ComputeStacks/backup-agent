package firewall

import (
	"fmt"
	"github.com/spf13/viper"
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
	cmd := exec.Command("bash", "-c", iptablesSaveCmd()+" | grep '\\-A expose-ports'")
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

func hostForwardIPTableRules() (rules []string) {
	cmd := exec.Command("bash", "-c", iptablesSaveCmd()+" | grep '\\-A container-inbound'")
	output, _ := cmd.CombinedOutput()
	if string(output) == "" {
		return []string{}
	}
	rawSplit := strings.Split(string(output), "\n")
	for _, v := range rawSplit {
		if strings.Contains(v, "container-inbound") {
			rules = append(rules, v)
		}
	}
	return rules
}

func deleteHostRule(line string) {
	l := strings.ReplaceAll(line, "-A", "-D")
	csFirewallLog().Info("Deleting Nat Rule", "rule", l)

	execCmd := fmt.Sprintf("%s -t nat %s", iptablesCmd(), l)
	cmd := exec.Command("bash", "-c", execCmd)

	output, _ := cmd.CombinedOutput()
	if string(output) == "" {
		return
	}
	return
}

func deleteForwardHostRule(line string) {
	l := strings.ReplaceAll(line, "-A", "-D")
	csFirewallLog().Info("Deleting Forward Rule", "rule", l)

	execCmd := fmt.Sprintf("%s %s", iptablesCmd(), l)
	cmd := exec.Command("bash", "-c", execCmd)

	output, _ := cmd.CombinedOutput()
	if string(output) == "" {
		return
	}
	return
}

func iptablesCmd() string {
	if viper.GetString("host.iptables-cmd") == "iptables-legacy" {
		return "iptables-legacy"
	}
	return "iptables"
}

func iptablesSaveCmd() string {
	if viper.GetString("host.iptables-cmd") == "iptables-legacy" {
		return "iptables-legacy-save"
	}
	return "iptables-save"
}

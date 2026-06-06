package firewall

import (
	"fmt"
	"os/exec"
	"strings"
)

// Cross-project network isolation.
//
// Every ComputeStacks project network is created (on Docker >= 28) with
// gateway_mode_ipv4=nat-unprotected (controller: CreateBridgeNetworkService).
// That mode makes Docker emit, for each project bridge, a blanket
//
//	-A DOCKER ! -i br-X -o br-X -j ACCEPT
//
// which accepts *all* forwarded ingress to that bridge -- including traffic
// routed from a *different* project's bridge. The result is that any container
// (e.g. an ssh/bastion container) can reach containers in every other project
// on the same node. We keep nat-unprotected (the controller publishes ports
// out-of-band via the expose-ports / container-inbound chains, which plain
// "nat" mode would drop), and instead restore isolation in DOCKER-USER.
//
// DOCKER-USER is evaluated in the FORWARD chain *before* DOCKER-FORWARD, and
// Docker never flushes its contents, so it is the supported place for this.
// Two rules:
//
//  1. RETURN packets that are being *bridged* (L2, same bridge) -- this is
//     intra-project container-to-container traffic, which must keep working.
//     With net.bridge.bridge-nf-call-iptables=1 (true on all nodes) those
//     frames traverse FORWARD as "-i br-X -o br-X"; --physdev-is-bridged is the
//     canonical way to distinguish bridged (L2) from routed (L3) here.
//  2. DROP everything else that both enters from and leaves to a docker bridge
//     -- i.e. *routed* bridge-to-bridge traffic, which is cross-project.
//
// Published-port ingress is unaffected: it arrives on a non-bridge interface
// (WAN/tailscale) after DNAT, so rule 2's "-i br-+" never matches it. Host ->
// container management traffic is OUTPUT, not FORWARD, so it is untouched. The
// interface-wildcard form is deliberately subnet-agnostic (project supernets
// differ per region: 10.100/16, 10.167/16, ...), so nothing here is hardcoded.
//
// INVARIANT: this assumes exactly one project per Linux bridge (enforced by the
// controller: Deployment has_one :private_network, one docker bridge network per
// project). Rule 1 RETURNs *any* L2-bridged packet, so if two projects ever
// shared a bridge, intra-bridge traffic between them would escape isolation.
// Rule 2 also treats ALL br-* bridges as mutually isolated -- there is no
// allow-list, so a future shared/infra bridge that must reach project
// containers would need an explicit "-i br-INFRA -o br-+ -j RETURN" above the
// DROP. IPv6 is intentionally not handled here: project bridge networks are
// IPv4-only (controller rejects IPv6 subnets); revisit with an ip6tables
// variant if that ever changes.
const (
	isoChain      = "DOCKER-USER"
	isoReturnSpec = "-m physdev --physdev-is-bridged -j RETURN"
	isoDropSpec   = "-i br-+ -o br-+ -j DROP"

	// Bound on the normalize loop. Real chains have 0-2 stray copies; the bound
	// only guards against a pathological/looping state.
	maxIsolationCleanupPasses = 8
)

// ensureProjectIsolation makes DOCKER-USER hold the two isolation rules above,
// in the correct order, idempotently. It is safe to call on every reconcile.
func ensureProjectIsolation() {
	// DOCKER-USER is created by dockerd. If it is not present yet (docker not
	// started), skip; the next reconcile / boot run will install the rules.
	if !chainExists(isoChain) {
		csFirewallLog().Warn("DOCKER-USER not present; skipping cross-project isolation rules")
		return
	}

	lines := chainRules(isoChain)
	retIdx := ruleIndex(lines, isoReturnSpec)
	dropIdx := ruleIndex(lines, isoDropSpec)

	// Already correct: both present and RETURN precedes DROP. Do nothing so we
	// never momentarily remove the isolation during steady-state reconciles.
	if retIdx >= 0 && dropIdx >= 0 && retIdx < dropIdx {
		return
	}

	csFirewallLog().Info("Enforcing cross-project network isolation in DOCKER-USER")

	// Remove any stray/misordered copies (bounded, in case of duplicates), then
	// insert both at the top in the correct order. Inserting DROP at position 1
	// first and then RETURN at position 1 leaves RETURN@1, DROP@2 -- ahead of
	// anything else already in the chain.
	for i := 0; i < maxIsolationCleanupPasses; i++ {
		lines = chainRules(isoChain)
		retIdx = ruleIndex(lines, isoReturnSpec)
		dropIdx = ruleIndex(lines, isoDropSpec)
		if retIdx < 0 && dropIdx < 0 {
			break
		}
		if retIdx >= 0 {
			_ = runIptables("-D " + isoChain + " " + isoReturnSpec)
		}
		if dropIdx >= 0 {
			_ = runIptables("-D " + isoChain + " " + isoDropSpec)
		}
	}
	if err := runIptables("-I " + isoChain + " 1 " + isoDropSpec); err != nil {
		csFirewallLog().Warn("failed to insert cross-project isolation DROP", "err", err.Error())
	}
	if err := runIptables("-I " + isoChain + " 1 " + isoReturnSpec); err != nil {
		// Most likely cause: xt_physdev not loadable on this kernel.
		csFirewallLog().Warn("failed to insert cross-project isolation RETURN (xt_physdev missing?)", "err", err.Error())
	}

	// Post-condition: if the rules did not end up present and correctly ordered
	// (failed insert, or a rule-rendering mismatch that would otherwise make us
	// churn delete+re-insert every reconcile), surface it loudly.
	lines = chainRules(isoChain)
	if r, d := ruleIndex(lines, isoReturnSpec), ruleIndex(lines, isoDropSpec); !(r >= 0 && d >= 0 && r < d) {
		csFirewallLog().Error("cross-project isolation rules not present/ordered after apply", "docker-user", strings.Join(lines, " | "))
	}
}

func chainExists(chain string) bool {
	return exec.Command("bash", "-c", fmt.Sprintf("%s -S %s", iptablesCmd(), chain)).Run() == nil
}

// chainRules returns the `iptables -S <chain>` lines. An empty slice is returned
// on error as well; callers treat "absent" and "empty" the same (re-apply), and
// ensureProjectIsolation only calls this after chainExists has passed.
func chainRules(chain string) []string {
	out, err := exec.Command("bash", "-c", fmt.Sprintf("%s -S %s", iptablesCmd(), chain)).CombinedOutput()
	if err != nil {
		return []string{}
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

// ruleIndex returns the index of the first line containing spec, or -1. spec is
// the exact rule specification used to insert and delete the rule, so the
// "find", "delete" and "verify" paths can never diverge.
func ruleIndex(lines []string, spec string) int {
	for i, l := range lines {
		if strings.Contains(l, spec) {
			return i
		}
	}
	return -1
}

// runIptables runs `<iptables-cmd> <args>` and returns the error (if any). It
// matches the package convention of shelling out via bash; all args here are
// compile-time constants, so there is no untrusted input to quote.
func runIptables(args string) error {
	execCmd := fmt.Sprintf("%s %s", iptablesCmd(), args)
	if out, err := exec.Command("bash", "-c", execCmd).CombinedOutput(); err != nil {
		csFirewallLog().Debug("isolation iptables cmd failed", "cmd", execCmd, "out", string(out))
		return err
	}
	return nil
}

package firewall

import (
	"os"
	"testing"

	"github.com/google/nftables"
)

// fakeConn records the objects an applyPlan call would program, so the apply
// assembly (which chains/sets/rules get created, in what shape) can be asserted
// without a kernel. It implements nftConn.
type fakeConn struct {
	tablesAdded int
	tablesDel   int
	chains      []*nftables.Chain
	sets        map[string][]nftables.SetElement
	rules       []*nftables.Rule
	flushed     int
}

func newFakeConn() *fakeConn { return &fakeConn{sets: map[string][]nftables.SetElement{}} }

func (f *fakeConn) AddTable(t *nftables.Table) *nftables.Table { f.tablesAdded++; return t }
func (f *fakeConn) DelTable(t *nftables.Table)                 { f.tablesDel++ }
func (f *fakeConn) AddChain(c *nftables.Chain) *nftables.Chain {
	f.chains = append(f.chains, c)
	return c
}
func (f *fakeConn) AddSet(s *nftables.Set, v []nftables.SetElement) error {
	f.sets[s.Name] = v
	return nil
}
func (f *fakeConn) AddRule(r *nftables.Rule) *nftables.Rule { f.rules = append(f.rules, r); return r }
func (f *fakeConn) Flush() error                            { f.flushed++; return nil }

// applyPlan must always produce a well-formed table skeleton (atomic replace +
// two nat chains + two sets + two maps + the two DNAT rules per nat chain),
// regardless of how many published ports there are. cs_agent is DNAT-only: no
// forward/filter chain. This exercises the apply assembly without the kernel.
func TestApplyPlanAssembly(t *testing.T) {
	plan := buildPlan(&NatRules{Rules: []NatRule{
		{Proto: "tcp", Nat: 20000, Port: 80, Dest: "10.100.0.5"},
		{Proto: "udp", Nat: 20001, Port: 53, Dest: "10.100.0.6"},
		{Proto: "tcp", Nat: 20002, Port: 443, Dest: "10.100.0.7", Driver: "calico-node"},
	}})

	f := newFakeConn()
	if err := applyPlan(f, plan); err != nil {
		t.Fatalf("applyPlan: %v", err)
	}

	// Atomic replace: add, delete, add.
	if f.tablesAdded != 2 || f.tablesDel != 1 {
		t.Errorf("table churn: added=%d del=%d, want added=2 del=1", f.tablesAdded, f.tablesDel)
	}
	if f.flushed != 1 {
		t.Errorf("flushed=%d, want exactly 1 atomic batch", f.flushed)
	}

	// Two nat base chains: prerouting, output. No forward chain.
	gotChains := map[string]bool{}
	for _, c := range f.chains {
		gotChains[c.Name] = true
	}
	if len(f.chains) != 2 {
		t.Errorf("chains = %d, want 2 (prerouting, output)", len(f.chains))
	}
	for _, name := range []string{chainPrerouting, chainOutput} {
		if !gotChains[name] {
			t.Errorf("missing chain %q", name)
		}
	}

	// Sets/maps present.
	for _, name := range []string{setPublishedTCP, setPublishedUDP, mapDNATTCP, mapDNATUDP} {
		if _, ok := f.sets[name]; !ok {
			t.Errorf("missing set/map %q", name)
		}
	}
	// Published ports: 2 tcp (20000, 20002 -- Driver is a no-op, both
	// published), 1 udp.
	if got := len(f.sets[setPublishedTCP]); got != 2 {
		t.Errorf("published_tcp has %d elements, want 2", got)
	}
	if got := len(f.sets[setPublishedUDP]); got != 1 {
		t.Errorf("published_udp has %d elements, want 1", got)
	}

	// Rules: DNAT only, in prerouting+output for both protos (2 chains * 2
	// protos = 4). No forward-accept rules.
	if len(f.rules) != 4 {
		t.Errorf("rule count = %d, want 4 (DNAT only)", len(f.rules))
	}
}

// Empty desired state must still render the full skeleton with empty sets --
// fail-closed (table present, no ports open), not "skip rendering".
func TestApplyPlanEmptyIsFailClosed(t *testing.T) {
	f := newFakeConn()
	if err := applyPlan(f, buildPlan(nil)); err != nil {
		t.Fatalf("applyPlan(empty): %v", err)
	}
	if len(f.chains) != 2 {
		t.Errorf("chains = %d, want 2 even when empty", len(f.chains))
	}
	for _, name := range []string{setPublishedTCP, setPublishedUDP} {
		if len(f.sets[name]) != 0 {
			t.Errorf("set %q should be empty, got %d elements", name, len(f.sets[name]))
		}
	}
	// Only the 4 DNAT rules; no published ports are open.
	if len(f.rules) != 4 {
		t.Errorf("rule count = %d, want 4 (DNAT only)", len(f.rules))
	}
}

// TestRenderTableKernel exercises the real netlink apply against the live
// kernel. It is gated off by default -- this sandbox has no NET_ADMIN /
// nf_tables, and `go test ./...` must stay green here. Run on a real node with
// FIREWALL_NFT_KERNEL_TEST=1 (and CAP_NET_ADMIN).
func TestRenderTableKernel(t *testing.T) {
	if os.Getenv("FIREWALL_NFT_KERNEL_TEST") != "1" {
		t.Skip("kernel nftables apply test; set FIREWALL_NFT_KERNEL_TEST=1 on a node with NET_ADMIN to run")
	}
	rules := &NatRules{Rules: []NatRule{
		{Proto: "tcp", Nat: 29999, Port: 80, Dest: "127.0.0.2"},
	}}
	if err := renderTable(rules); err != nil {
		t.Fatalf("renderTable: %v", err)
	}
	// Tear down what we created so the test is repeatable.
	if err := renderTable(nil); err != nil {
		t.Fatalf("renderTable(nil) teardown: %v", err)
	}
}

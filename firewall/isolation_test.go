package firewall

import "testing"

// ruleIndex must match a rule only by its exact spec, so a decoy rule that
// merely shares tokens (e.g. another "br-+ ... DROP") is not mistaken for ours.
func TestRuleIndex(t *testing.T) {
	ret := "-A DOCKER-USER " + isoReturnSpec
	drop := "-A DOCKER-USER " + isoDropSpec

	cases := []struct {
		name   string
		lines  []string
		spec   string
		expect int
	}{
		{"empty chain (only -N)", []string{"-N DOCKER-USER"}, isoReturnSpec, -1},
		{"return present", []string{"-N DOCKER-USER", ret, drop}, isoReturnSpec, 1},
		{"drop present", []string{"-N DOCKER-USER", ret, drop}, isoDropSpec, 2},
		{"decoy drop with shared tokens does not match", []string{"-N DOCKER-USER", "-A DOCKER-USER -i br-+ -p tcp --dport 22 -j DROP"}, isoDropSpec, -1},
		{"bare docker RETURN is not the physdev RETURN", []string{"-N DOCKER-USER", "-A DOCKER-USER -j RETURN"}, isoReturnSpec, -1},
		{"not present", []string{"-N DOCKER-USER", ret}, isoDropSpec, -1},
	}
	for _, c := range cases {
		if got := ruleIndex(c.lines, c.spec); got != c.expect {
			t.Errorf("%s: ruleIndex(_, %q) = %d, want %d", c.name, c.spec, got, c.expect)
		}
	}
}

// The fast-path "already correct" decision must be true only when both rules
// are present and RETURN precedes DROP, across all chain states the reconcile
// can encounter.
func TestIsolationOrderingDecision(t *testing.T) {
	correct := func(lines []string) bool {
		r, d := ruleIndex(lines, isoReturnSpec), ruleIndex(lines, isoDropSpec)
		return r >= 0 && d >= 0 && r < d
	}
	ret := "-A DOCKER-USER " + isoReturnSpec
	drop := "-A DOCKER-USER " + isoDropSpec

	cases := []struct {
		name   string
		lines  []string
		expect bool
	}{
		{"ordered", []string{"-N DOCKER-USER", ret, drop}, true},
		{"reversed", []string{"-N DOCKER-USER", drop, ret}, false},
		{"only return", []string{"-N DOCKER-USER", ret}, false},
		{"only drop", []string{"-N DOCKER-USER", drop}, false},
		{"empty", []string{"-N DOCKER-USER"}, false},
		{"ordered with trailing docker RETURN", []string{"-N DOCKER-USER", ret, drop, "-A DOCKER-USER -j RETURN"}, true},
	}
	for _, c := range cases {
		if got := correct(c.lines); got != c.expect {
			t.Errorf("%s: correct=%v, want %v", c.name, got, c.expect)
		}
	}
}

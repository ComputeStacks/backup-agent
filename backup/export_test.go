package backup

import (
	"strings"
	"testing"
)

func TestSanitizeKeySegment(t *testing.T) {
	cases := map[string]string{
		"vol-1":           "vol-1",
		"my.archive_2024": "my.archive_2024",
		"bad/slash":       "bad_slash",
		"a b":             "a_b",
		"a;b":             "a_b",
	}
	for in, want := range cases {
		if got := sanitizeKeySegment(in); got != want {
			t.Errorf("sanitizeKeySegment(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRandomTokenDistinctHex(t *testing.T) {
	a, b := randomToken(), randomToken()
	if a == b {
		t.Error("expected distinct random tokens")
	}
	if len(a) != 24 { // 12 random bytes -> 24 hex chars
		t.Errorf("token length = %d, want 24", len(a))
	}
	if strings.Trim(a, "0123456789abcdef") != "" {
		t.Errorf("token is not lowercase hex: %q", a)
	}
}

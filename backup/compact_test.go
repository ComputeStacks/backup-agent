package backup

import (
	"testing"
	"time"
)

func TestJitterDelay(t *testing.T) {
	// Deterministic for a given hostname.
	if jitterDelay("node-a", 1800) != jitterDelay("node-a", 1800) {
		t.Error("jitterDelay should be deterministic for the same hostname")
	}
	// Always within [0, maxSec).
	for _, host := range []string{"node-a", "node-b", "compute-01", ""} {
		d := jitterDelay(host, 1800)
		if d < 0 || d >= 1800*time.Second {
			t.Errorf("jitterDelay(%q) = %v, want within [0, 1800s)", host, d)
		}
	}
	// maxSec <= 0 disables jitter.
	if jitterDelay("node-a", 0) != 0 {
		t.Error("jitterDelay with maxSec=0 should be 0")
	}
}

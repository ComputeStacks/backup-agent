package backup

import (
	"context"
	"testing"
	"time"
)

// TestHousekeeper_UntilNextFallback covers the m4 detail: changelog.prune_freq is a
// CRON, and an empty/unparseable cron must fall back to a fixed interval so
// retention can't be silently disabled.
func TestHousekeeper_UntilNextFallback(t *testing.T) {
	h := &Housekeeper{expr: ""}
	if d := h.untilNext(time.Now()); d != defaultHousekeepingInterval {
		t.Fatalf("empty cron: untilNext = %v, want %v", d, defaultHousekeepingInterval)
	}
	h.expr = "not a cron"
	if d := h.untilNext(time.Now()); d != defaultHousekeepingInterval {
		t.Fatalf("bad cron: untilNext = %v, want %v", d, defaultHousekeepingInterval)
	}
	h.expr = "*/15 * * * *"
	if d := h.untilNext(time.Now()); d <= 0 || d > 16*time.Minute {
		t.Fatalf("valid cron: untilNext = %v, want in (0, 16m]", d)
	}
}

// TestHousekeeper_RunOnce proves housekeeping runs its retention passes directly
// (no scheduler, no backups.enabled gate) without error on an empty control.db.
func TestHousekeeper_RunOnce(t *testing.T) {
	st := testStore(t)
	NewHousekeeper(st).runOnce(context.Background())
}

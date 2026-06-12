package job

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestExportInFlightDedup(t *testing.T) {
	id := "jobs/abc"
	clearExportInFlight(id)
	if !markExportInFlight(id) {
		t.Fatal("first mark should succeed")
	}
	if markExportInFlight(id) {
		t.Error("second mark of an in-flight id should fail")
	}
	clearExportInFlight(id)
	if !markExportInFlight(id) {
		t.Error("after clear, mark should succeed again")
	}
	clearExportInFlight(id)
}

// Exactly one of N concurrent marks of the same id wins. Run with -race.
func TestExportInFlightConcurrent(t *testing.T) {
	const n = 16
	id := "jobs/concurrent"
	clearExportInFlight(id)
	var wins int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if markExportInFlight(id) {
				atomic.AddInt32(&wins, 1)
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Errorf("expected exactly 1 winner, got %d", wins)
	}
	clearExportInFlight(id)
}

package backup

import (
	"os"
	"strings"
	"testing"
)

// TestTaskHandlersHaveNoRecover guards the B1 fix. A task handler must NOT contain
// a handler-level `defer sentry.Recover()`: sentry-go's package-level Recover()
// calls recover() and does not re-panic, so it would swallow a panic before it
// reaches the worker's terminal guard (job/worker.go) and the task would be
// recorded as a false "completed" on the never-replay kinds. A panic must instead
// propagate out of RunTask so the guard marks the task failed. If this test fails,
// a regression re-added the swallowing defer.
func TestTaskHandlersHaveNoRecover(t *testing.T) {
	for _, f := range []string{"backup.go", "restore.go", "export.go", "trash.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if strings.Contains(string(src), "defer sentry.Recover()") {
			t.Errorf("%s has a handler-level `defer sentry.Recover()`; a task handler must let a "+
				"panic reach the worker terminal guard (B1) — remove it", f)
		}
	}
}

package job

import "sync"

// Export jobs are long-running and their Consul KV key is NOT deleted at
// dispatch (it survives until ExportBackup finishes). The blocking Keys watch
// would otherwise re-list — and re-dispatch — the same key on every tick. This
// in-flight set dedups: a job is marked before hand-off and cleared only after
// it completes (and after its KV key is deleted).
var (
	exportInFlightMu sync.Mutex
	exportInFlight   = map[string]struct{}{}
)

// markExportInFlight records jobID as in-flight, returning false if it already was.
func markExportInFlight(jobID string) bool {
	exportInFlightMu.Lock()
	defer exportInFlightMu.Unlock()
	if _, ok := exportInFlight[jobID]; ok {
		return false
	}
	exportInFlight[jobID] = struct{}{}
	return true
}

// clearExportInFlight removes jobID from the in-flight set.
func clearExportInFlight(jobID string) {
	exportInFlightMu.Lock()
	defer exportInFlightMu.Unlock()
	delete(exportInFlight, jobID)
}

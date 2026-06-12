package borg

import "sync"

// Per-repository serialization for operations that must not run concurrently
// against the same borg repository within this agent process.
//
// The registry holds one mutex per repository name, created on first use. The
// outer mutex only guards the map itself; the per-repo mutex is what callers
// actually hold during their borg operation.
var (
	repoLocksMu sync.Mutex
	repoLocks   = map[string]*sync.Mutex{}
)

func repoLock(name string) *sync.Mutex {
	repoLocksMu.Lock()
	defer repoLocksMu.Unlock()
	m, ok := repoLocks[name]
	if !ok {
		m = &sync.Mutex{}
		repoLocks[name] = m
	}
	return m
}

// AcquireRepoLock blocks until the per-repository lock for name is held and
// returns a function that releases it. Typical use:
//
//	defer borg.AcquireRepoLock(vol.Name)()
//
// This serializes the operations that mutate (or, for export, read while
// bypassing borg's own lock) the same repository: export, prune, and compact.
// It is intentionally NOT taken by backup creation (`borg create`): create is
// append-only and safe to run alongside a bypass-lock export, and forcing it to
// wait would risk missing a scheduled backup.
//
// The lock is process-local. It is sufficient only because every repository is
// owned by exactly one node (enforced by the vol.Node == hostname guards in the
// backup package). If a volume could ever be served by two nodes at once this
// provides no protection and a repository-level guard would be required.
func AcquireRepoLock(name string) func() {
	m := repoLock(name)
	m.Lock()
	return m.Unlock
}

package borg

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Same key must serialize: at most one holder at a time. Run with -race.
func TestRepoLockSameKeySerializes(t *testing.T) {
	const name = "vol-a"
	var concurrent, violated int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer AcquireRepoLock(name)()
			if atomic.AddInt32(&concurrent, 1) > 1 {
				atomic.StoreInt32(&violated, 1)
			}
			time.Sleep(2 * time.Millisecond)
			atomic.AddInt32(&concurrent, -1)
		}()
	}
	wg.Wait()
	if violated != 0 {
		t.Error("same-key lock allowed more than one concurrent holder")
	}
}

// A different key must not block while another key is held.
func TestRepoLockDistinctKeysParallel(t *testing.T) {
	release := AcquireRepoLock("alpha")
	defer release()

	done := make(chan struct{})
	go func() {
		AcquireRepoLock("beta")()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("distinct-key lock blocked while a different key was held")
	}
}

func TestRepoLockRegistry(t *testing.T) {
	if repoLock("same") != repoLock("same") {
		t.Error("repoLock should return the same mutex for the same name")
	}
	if repoLock("one") == repoLock("two") {
		t.Error("repoLock should return distinct mutexes for distinct names")
	}
}

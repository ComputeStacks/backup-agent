package job

import (
	"context"
	"cs-agent/types"
	"sync"
	"testing"
	"time"

	consulAPI "github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
)

func TestWorker(t *testing.T) {
	// Setup
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	queue := make(chan types.Job, 1)
	processed := make(chan string, 1)

	// Mock processor
	mockProcessor := func(consul *consulAPI.Client, job *types.Job) {
		processed <- job.ID
	}

	// Start worker
	wg.Add(1)
	go worker(ctx, &wg, "test-queue", nil, queue, mockProcessor)

	// Test job processing
	jobID := "test-job-1"
	queue <- types.Job{ID: jobID}

	select {
	case id := <-processed:
		assert.Equal(t, jobID, id)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for job to be processed")
	}

	// Test shutdown
	cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for worker to shutdown")
	}
}

package job

import (
	"context"
	"cs-agent/types"
	"sync"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
)

func setupWorkers(ctx context.Context, wg *sync.WaitGroup, consul *consulAPI.Client, queueName string, workerCount int, jobQueue chan types.Job) {
	for i := 1; i <= workerCount; i++ {
		jobEvent().Info("Starting worker process", "queue", queueName, "worker-process", i)
		go worker(ctx, wg, queueName, consul, jobQueue)
	}
	return
}

func worker(ctx context.Context, wg *sync.WaitGroup, queueName string, consul *consulAPI.Client, queue <-chan types.Job) {
	defer sentry.Recover()
	defer wg.Done()
	defer func() {
		jobEvent().Info("Worker stopping...")
	}()
	for {
		select {
		case <-ctx.Done():
			jobEvent().Info("[" + queueName + "] Shutting down")
			return
		case job := <-queue:
			//jobEvent().Info("["+queueName+"] Have job", "job", job.ID)
			processJob(consul, &job)
			if ctx.Err() != nil {
				jobEvent().Info("[" + queueName + "] Shutdown")
				return
			}
		}
	}
}

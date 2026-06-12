package job

import (
	"context"
	"cs-agent/types"
	"sync"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
)

type JobProcessor func(consul *consulAPI.Client, job *types.Job)

func setupWorkers(ctx context.Context, wg *sync.WaitGroup, consul *consulAPI.Client, queueName string, workerCount int, jobQueue chan types.Job) {
	wg.Add(workerCount) // register this pool's workers; main only Adds job.Watch itself
	for i := 1; i <= workerCount; i++ {
		jobEvent().Info("Starting worker process", "queue", queueName, "worker-process", i)
		go worker(ctx, wg, queueName, consul, jobQueue, processJob)
	}

}

func worker(ctx context.Context, wg *sync.WaitGroup, queueName string, consul *consulAPI.Client, queue <-chan types.Job, processor JobProcessor) {
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
			processor(consul, &job)
			if ctx.Err() != nil {
				jobEvent().Info("[" + queueName + "] Shutdown")
				return
			}
		}
	}
}

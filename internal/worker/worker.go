package worker

import (
	"context"
	"distributed-task-queue/internal/job"
	"distributed-task-queue/internal/storage"
	"time"
	"log"
)

type HandlerFunc func(ctx context.Context, j *job.Job) error

type Worker struct {
	store    *storage.Store
	handlers map[string]HandlerFunc
	interval time.Duration
}

func New(store *storage.Store, interval time.Duration) *Worker {
	return &Worker{
		store:    store,
		handlers: make(map[string]HandlerFunc),
		interval: interval,
	}
}

func (w *Worker) Register(jobType string, handler HandlerFunc) {
	w.handlers[jobType] = handler
}

func (w *Worker) executeJob(ctx context.Context, j *job.Job) error {
    handler, ok := w.handlers[j.Type]
    if !ok {
        return w.store.UpdateJob(ctx, storage.UpdateJobParams{
            ID:          j.ID,
            Status:      job.StatusDead,
            Attempts:    j.Attempts + 1,
            ErrorMsg:    "no handler registered for job type: " + j.Type,
            ScheduledAt: j.ScheduledAt,
        })
    }

    err := handler(ctx, j)

    if err == nil {
        return w.store.UpdateJob(ctx, storage.UpdateJobParams{
            ID:          j.ID,
            Status:      job.StatusCompleted,
            Attempts:    j.Attempts,
            ScheduledAt: j.ScheduledAt,
        })
    }

    newAttempts := j.Attempts + 1
    if newAttempts >= j.MaxAttempts {
        return w.store.UpdateJob(ctx, storage.UpdateJobParams{
            ID:          j.ID,
            Status:      job.StatusDead,
            Attempts:    newAttempts,
            ErrorMsg:    err.Error(),
            ScheduledAt: j.ScheduledAt,
        })
    }

    backoff := time.Duration(newAttempts) * 10 * time.Second
    return w.store.UpdateJob(ctx, storage.UpdateJobParams{
        ID:          j.ID,
        Status:      job.StatusPending,
        Attempts:    newAttempts,
        ErrorMsg:    err.Error(),
        ScheduledAt: time.Now().UTC().Add(backoff),
    })
}

func (w *Worker) Start(ctx context.Context) {
    idledelay := 2 * time.Second
    for {
        if ctx.Err() != nil {
            return
        }

        j, err := w.store.ClaimJob(ctx)
        if err != nil {
            log.Printf("error claiming job: %v\n", err)
            time.Sleep(idledelay) // wait and retry 
            continue
        }

        if j == nil {
            select { // whichever happens first 
            case <-ctx.Done():
                return
            case <-time.After(idledelay):
                continue
            }
        }

        if err := w.executeJob(ctx, j); err != nil {
            log.Printf("job %d failed: %v\n", j.ID, err)
        }
    }
}
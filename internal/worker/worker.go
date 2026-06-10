package worker

import (
	"context"
	"distributed-task-queue/internal/job"
	"distributed-task-queue/internal/storage"
	"time"
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
		j.Attempts++
		j.Status = job.StatusFailed
		j.ErrorMsg = "no handler registered for this job type"
		return w.store.UpdateJob(ctx, storage.UpdateJobParams{
			ID:          j.ID,
			Attempts:    j.Attempts,
			Status:      j.Status,
			ErrorMsg:    j.ErrorMsg,
			ScheduledAt: j.ScheduledAt,
		})
	}

	err := handler(ctx, j)
	j.Attempts++

	if err == nil {
		j.Status = job.StatusCompleted
		j.ErrorMsg = ""
	} else {
		j.ErrorMsg = err.Error()
		if j.Attempts >= j.MaxAttempts {
			j.Status = job.StatusFailed
		} else {
			j.Status = job.StatusPending
			j.ScheduledAt = time.Now().UTC().Add(time.Duration(j.Attempts) * 10 * time.Second)
		}
	}

	return w.store.UpdateJob(ctx, storage.UpdateJobParams{
		ID:          j.ID,
		Attempts:    j.Attempts,
		Status:      j.Status,
		ErrorMsg:    j.ErrorMsg,
		ScheduledAt: j.ScheduledAt,
	})
}

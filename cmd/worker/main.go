package main

import (
	"context"
	"distributed-task-queue/internal/config"
	"distributed-task-queue/internal/job"
	"distributed-task-queue/internal/storage"
	"distributed-task-queue/internal/worker"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT) // process termination , force kill
	go func() {
		<-quit
		log.Println("shutdown signal received")
		cancel()
	}()

	dbPool, err := storage.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer dbPool.Close()

	store := storage.NewStore(dbPool)
	w := worker.New(store, 5*time.Second)

	w.Register("send_email", func(ctx context.Context, j *job.Job) error {
		log.Printf("processing send_email job %d: %s\n", j.ID, string(j.Payload))
		return nil
	})

	log.Println("worker started, polling for jobs")
	w.Start(ctx)
	log.Println("worker exited cleanly")
}

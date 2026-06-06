package main

import (
	"context"
	"distributed-task-queue/internal/config"
	"distributed-task-queue/internal/storage"
	"log"
	"time"
)

func main() {

	// context.WithTimeout returns two things: ctx and cancel 
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	db, err := storage.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_ = storage.NewStore(db)

	log.Println("connected to db ")

}

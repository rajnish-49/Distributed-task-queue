package main

import (
	"context"
	"distributed-task-queue/internal/config"
	handlerhttp "distributed-task-queue/internal/http"
	"distributed-task-queue/internal/storage"
	"log"
	"net/http"
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

	store := storage.NewStore(db)
	handler := handlerhttp.NewHandler(store)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", handler.CreateJob)
	mux.HandleFunc("GET /jobs/{id}", handler.GetJob)

	log.Println("api listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}

}

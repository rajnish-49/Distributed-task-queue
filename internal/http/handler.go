package http

import (
	"encoding/json"
	"net/http"

	"distributed-task-queue/internal/storage"
)

type Handler struct {
	store *storage.Store
}

func NewHandler(store *storage.Store) *Handler {
	return &Handler{
		store: store,
	}
}

type createJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	createdJob, err := h.store.CreateJob(r.Context(), storage.CreateJobParams{
		Type:        req.Type,
		Payload:     req.Payload,
		MaxAttempts: req.MaxAttempts,
	})
	if err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(createdJob)
}

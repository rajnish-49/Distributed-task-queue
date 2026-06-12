package http

import (
	"distributed-task-queue/internal/storage"
	"encoding/json"
	"net/http"
	"strconv"
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

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid job ID", http.StatusBadRequest)
		return
	}

	job, err := h.store.GetJobByID(r.Context(), id)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if job == nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(job); err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

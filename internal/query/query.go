package query

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"

	"github.com/daadaamed/go-cqrs-pubsub/internal/store"
)

// ReadModel is the slice of the store this package needs (consumer-defined).
type ReadModel interface {
	ListTodos(ctx context.Context) ([]store.Todo, error)
	GetTodo(ctx context.Context, id uuid.UUID) (*store.Todo, error)
}

// Handler serves read-side queries.
type Handler struct {
	rm ReadModel
}

// NewHandler wires the query handler with its read-model dependency.
func NewHandler(rm ReadModel) *Handler {
	return &Handler{rm: rm}
}

// ListTodos handles GET /todos.
func (h *Handler) ListTodos(w http.ResponseWriter, r *http.Request) {
	todos, err := h.rm.ListTodos(r.Context())
	if err != nil {
		log.Printf("query: list todos: %v", err)
		writeError(w, http.StatusInternalServerError, "could not list todos")
		return
	}
	writeJSON(w, http.StatusOK, todos)
}

// GetTodo handles GET /todos/{id}.
func (h *Handler) GetTodo(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	todo, err := h.rm.GetTodo(r.Context(), id)
	if err != nil {
		log.Printf("query: get todo: %v", err)
		writeError(w, http.StatusInternalServerError, "could not get todo")
		return
	}
	if todo == nil {
		writeError(w, http.StatusNotFound, "todo not found")
		return
	}
	writeJSON(w, http.StatusOK, todo)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

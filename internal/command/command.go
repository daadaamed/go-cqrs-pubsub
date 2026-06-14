// Package command holds the write-side HTTP handlers. A command validates
// input, produces a domain event, and appends it to the event store. It never
// writes read-model state directly — that is the projection's job (later phase).
package command

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/daadaamed/todo-cqrs/internal/event"
)

// EventAppender is the slice of the store this package needs. Defining the
// interface here (at the consumer) keeps the dependency small and testable.
type EventAppender interface {
	AppendEvent(ctx context.Context, e *event.Event) error
}

// Handler serves write-side commands.
type Handler struct {
	events EventAppender
}

// NewHandler wires the command handler with its dependencies.
func NewHandler(events EventAppender) *Handler {
	return &Handler{events: events}
}

type createTodoRequest struct {
	Title string `json:"title"`
}

type createTodoResponse struct {
	ID string `json:"id"`
}

// CreateTodo handles POST /todos: validate -> build TodoCreated -> append.
func (h *Handler) CreateTodo(w http.ResponseWriter, r *http.Request) {
	var req createTodoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	title := strings.TrimSpace(req.Title)
	if title == "" {
		writeError(w, http.StatusBadRequest, "title is required")
		return
	}

	payload, err := json.Marshal(event.TodoCreatedPayload{Title: title})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encode payload")
		return
	}

	e := &event.Event{
		AggregateID: uuid.New(),
		Type:        event.TypeTodoCreated,
		Payload:     payload,
	}

	if err := h.events.AppendEvent(r.Context(), e); err != nil {
		// Wrap for server logs; keep the client message generic.
		writeError(w, http.StatusInternalServerError, "could not create todo")
		_ = fmt.Errorf("create todo: %w", err)
		return
	}

	writeJSON(w, http.StatusCreated, createTodoResponse{ID: e.AggregateID.String()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// Package command holds the write-side HTTP handlers. A command validates
// input, produces a domain event, appends it to the event store (source of
// truth), then publishes it for the read side to project. It never writes
// read-model state directly.
package command

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/daadaamed/go-cqrs-pubsub/internal/event"
)

// EventAppender is the write side of the event store (consumer-defined interface).
type EventAppender interface {
	AppendEvent(ctx context.Context, e *event.Event) error
}

// EventPublisher publishes an event to the message bus (consumer-defined interface).
type EventPublisher interface {
	Publish(ctx context.Context, e *event.Event) error
}

// Handler serves write-side commands.
type Handler struct {
	events    EventAppender
	publisher EventPublisher
}

// NewHandler wires the command handler with its dependencies.
func NewHandler(events EventAppender, publisher EventPublisher) *Handler {
	return &Handler{events: events, publisher: publisher}
}

type createTodoRequest struct {
	Title string `json:"title"`
}

type createTodoResponse struct {
	ID string `json:"id"`
}

// CreateTodo handles POST /todos: validate -> build TodoCreated -> append -> publish.
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

	// 1) Append to the store — this is the durable source of truth.
	if err := h.events.AppendEvent(r.Context(), e); err != nil {
		log.Printf("create todo: append: %v", err)
		writeError(w, http.StatusInternalServerError, "could not create todo")
		return
	}

	// 2) Publish for the read side. If this fails the event is still stored and
	// can be replayed later; we log and report success for the command since the
	// write is durable. (A stricter design would use a transactional outbox.)
	if err := h.publisher.Publish(r.Context(), e); err != nil {
		log.Printf("create todo: publish (event %s persisted, will lag until replay): %v", e.AggregateID, err)
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

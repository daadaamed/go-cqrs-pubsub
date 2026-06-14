// Package projection is the read-side updater. It receives Pub/Sub push
// deliveries over HTTP, decodes the domain event, and writes the denormalized
// read model. Handlers are idempotent because Pub/Sub delivers at-least-once.
package projection

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"

	"github.com/daadaamed/go-cqrs-pubsub/internal/event"
)

// ReadModelWriter is the slice of the store the projection needs (consumer-defined).
type ReadModelWriter interface {
	UpsertTodo(ctx context.Context, id uuid.UUID, title string) error
}

// Handler serves the Pub/Sub push endpoint.
type Handler struct {
	rm ReadModelWriter
}

// NewHandler wires the projection with its read-model dependency.
func NewHandler(rm ReadModelWriter) *Handler {
	return &Handler{rm: rm}
}

// pushEnvelope is the JSON Pub/Sub POSTs to a push endpoint. The event payload
// is base64 in message.data; attributes carry type and aggregate_id.
type pushEnvelope struct {
	Message struct {
		Data       []byte            `json:"data"` // base64 auto-decoded into []byte
		Attributes map[string]string `json:"attributes"`
		MessageID  string            `json:"messageId"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

// HandleEvent processes one push delivery. Returns 2xx on success (Pub/Sub
// acks); 5xx on transient failure (Pub/Sub retries); 2xx on malformed/unknown
// messages to avoid poison-message loops.
func (h *Handler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	var env pushEnvelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		log.Printf("projection: decode envelope: %v", err)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	evType := event.Type(env.Message.Attributes["type"])
	aggStr := env.Message.Attributes["aggregate_id"]
	aggID, err := uuid.Parse(aggStr)
	if err != nil {
		log.Printf("projection: bad aggregate_id %q: %v", aggStr, err)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	switch evType {
	case event.TypeTodoCreated:
		var p event.TodoCreatedPayload
		if err := json.Unmarshal(env.Message.Data, &p); err != nil {
			log.Printf("projection: decode TodoCreated payload: %v", err)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err := h.rm.UpsertTodo(r.Context(), aggID, p.Title); err != nil {
			log.Printf("projection: upsert todo: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Printf("projection: applied TodoCreated id=%s title=%q", aggID, p.Title)

	default:
		log.Printf("projection: ignoring unknown event type %q", evType)
	}

	w.WriteHeader(http.StatusNoContent)
}

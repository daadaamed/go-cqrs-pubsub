// Package event defines domain event types and the envelope used across the
// write side. Events are the source of truth: commands produce them, the store
// persists them append-only, and the read model is derived from them.
package event

import (
	"time"

	"github.com/google/uuid"
)

// Type enumerates the domain events. Add new ones here as the model grows.
type Type string

const (
	TypeTodoCreated Type = "TodoCreated"
	// TypeTodoCompleted is introduced in a later phase.
)

// Event is the persisted envelope. ID and CreatedAt are assigned by the store
// on append (ID via BIGSERIAL, CreatedAt via DB default), so they are zero
// until then.
type Event struct {
	ID          int64     // global ordering, set by the store
	AggregateID uuid.UUID // which todo this event concerns
	Type        Type      // discriminates the payload
	Payload     []byte    // JSON-encoded, shape depends on Type
	CreatedAt   time.Time // set by the store
}

// TodoCreatedPayload is the JSON shape stored in Event.Payload for TypeTodoCreated.
type TodoCreatedPayload struct {
	Title string `json:"title"`
}

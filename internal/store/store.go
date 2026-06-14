// Package store is the pgx-backed persistence layer. It exposes the write side
// (appending events) and the read-model write path (upserting todos).
package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daadaamed/go-cqrs-pubsub/internal/event"
)

// Store wraps a pgx connection pool. Construct it with New; pass it explicitly
// to the handlers that need it (no globals).
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool against databaseURL and verifies connectivity.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool. Safe to call once during shutdown.
func (s *Store) Close() {
	s.pool.Close()
}

// AppendEvent inserts one event and back-fills the DB-assigned ID and CreatedAt
// onto the passed event. The events table is append-only: we only ever INSERT.
func (s *Store) AppendEvent(ctx context.Context, e *event.Event) error {
	const q = `
		INSERT INTO events (aggregate_id, type, payload)
		VALUES ($1, $2, $3)
		RETURNING id, created_at`
	err := s.pool.QueryRow(ctx, q, e.AggregateID, string(e.Type), e.Payload).
		Scan(&e.ID, &e.CreatedAt)
	if err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	return nil
}

// UpsertTodo writes the read model for one todo. It is idempotent: re-applying
// the same event (Pub/Sub is at-least-once) produces the same row. This is the
// projection's only write path in this phase.
func (s *Store) UpsertTodo(ctx context.Context, id uuid.UUID, title string) error {
	const q = `
		INSERT INTO todos_read (id, title, done, updated_at)
		VALUES ($1, $2, false, now())
		ON CONFLICT (id) DO UPDATE
		SET title = EXCLUDED.title, updated_at = now()`
	if _, err := s.pool.Exec(ctx, q, id, title); err != nil {
		return fmt.Errorf("upsert todo: %w", err)
	}
	return nil
}

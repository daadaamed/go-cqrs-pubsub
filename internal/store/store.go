// Package store is the pgx-backed persistence layer. In this phase it exposes
// the write side only: appending events to the append-only event store.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daadaamed/todo-cqrs/internal/event"
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

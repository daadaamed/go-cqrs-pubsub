package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

// Todo is a read-model row returned by the query side.
type Todo struct {
	ID        uuid.UUID `json:"id"`
	Title     string    `json:"title"`
	Done      bool      `json:"done"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ListTodos returns all read-model rows, newest first. Read side only — never
// reads from the events table.
func (s *Store) ListTodos(ctx context.Context) ([]Todo, error) {
	const q = `SELECT id, title, done, updated_at FROM todos_read ORDER BY updated_at DESC`
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list todos: %w", err)
	}
	defer rows.Close()

	todos := make([]Todo, 0)
	for rows.Next() {
		var t Todo
		if err := rows.Scan(&t.ID, &t.Title, &t.Done, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan todo: %w", err)
		}
		todos = append(todos, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate todos: %w", err)
	}
	return todos, nil
}

// GetTodo returns one read-model row by id. Returns (nil, nil) when not found,
// letting the handler map that to 404.
func (s *Store) GetTodo(ctx context.Context, id uuid.UUID) (*Todo, error) {
	const q = `SELECT id, title, done, updated_at FROM todos_read WHERE id = $1`
	var t Todo
	err := s.pool.QueryRow(ctx, q, id).Scan(&t.ID, &t.Title, &t.Done, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get todo: %w", err)
	}
	return &t, nil
}

func (s *Store) MarkDone(ctx context.Context, id uuid.UUID) (bool, error) {
	const q = `UPDATE todos_read SET done = true, updated_at = now() WHERE id = $1`
	tag, err := s.pool.Exec(ctx, q, id)
	if err != nil {
		return false, fmt.Errorf("mark done: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RebuildReadModel reconstructs todos_read entirely from the event log. It
// truncates the read model and replays every event in global (id) order inside
// a single transaction, so a mid-rebuild failure rolls back to the prior state.
// This is the proof that the read model is derived, not authoritative: events
// are the only source of truth. Returns the number of events applied.
func (s *Store) RebuildReadModel(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin rebuild tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after a successful Commit

	if _, err := tx.Exec(ctx, `TRUNCATE todos_read`); err != nil {
		return 0, fmt.Errorf("truncate read model: %w", err)
	}

	rows, err := tx.Query(ctx, `SELECT aggregate_id, type, payload FROM events ORDER BY id`)
	if err != nil {
		return 0, fmt.Errorf("read event log: %w", err)
	}
	defer rows.Close()

	// Collect first so we can run further statements on the same tx without an
	// open cursor conflict.
	type rawEvent struct {
		aggID   uuid.UUID
		evType  string
		payload []byte
	}
	var evs []rawEvent
	for rows.Next() {
		var e rawEvent
		if err := rows.Scan(&e.aggID, &e.evType, &e.payload); err != nil {
			return 0, fmt.Errorf("scan event: %w", err)
		}
		evs = append(evs, e)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate events: %w", err)
	}

	applied := 0
	for _, e := range evs {
		switch event.Type(e.evType) {
		case event.TypeTodoCreated:
			var p event.TodoCreatedPayload
			if err := json.Unmarshal(e.payload, &p); err != nil {
				return 0, fmt.Errorf("decode TodoCreated %s: %w", e.aggID, err)
			}
			const ins = `
				INSERT INTO todos_read (id, title, done, updated_at)
				VALUES ($1, $2, false, now())
				ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, updated_at = now()`
			if _, err := tx.Exec(ctx, ins, e.aggID, p.Title); err != nil {
				return 0, fmt.Errorf("replay TodoCreated %s: %w", e.aggID, err)
			}
		case event.TypeTodoCompleted:
			if _, err := tx.Exec(ctx, `UPDATE todos_read SET done = true, updated_at = now() WHERE id = $1`, e.aggID); err != nil {
				return 0, fmt.Errorf("replay TodoCompleted %s: %w", e.aggID, err)
			}
		default:
			// Unknown historical type: skip. Newer code may not know old events.
		}
		applied++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit rebuild: %w", err)
	}
	return applied, nil
}

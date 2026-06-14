package projection

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/daadaamed/go-cqrs-pubsub/internal/event"
)

type fakeRM struct {
	upserted  map[uuid.UUID]string
	marked    map[uuid.UUID]bool
	markMatch bool
	upsertErr error
	markErr   error
}

func newFakeRM() *fakeRM {
	return &fakeRM{
		upserted:  map[uuid.UUID]string{},
		marked:    map[uuid.UUID]bool{},
		markMatch: true,
	}
}

func (f *fakeRM) UpsertTodo(_ context.Context, id uuid.UUID, title string) error {
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.upserted[id] = title
	return nil
}

func (f *fakeRM) MarkDone(_ context.Context, id uuid.UUID) (bool, error) {
	if f.markErr != nil {
		return false, f.markErr
	}
	f.marked[id] = true
	return f.markMatch, nil
}

func pushBody(t *testing.T, evType event.Type, id uuid.UUID, data []byte) string {
	t.Helper()
	env := map[string]any{
		"message": map[string]any{
			"data":       data, // json marshals []byte as base64, matching Pub/Sub
			"attributes": map[string]string{"type": string(evType), "aggregate_id": id.String()},
		},
		"subscription": "test",
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestHandleEvent_TodoCreated(t *testing.T) {
	rm := newFakeRM()
	h := NewHandler(rm)
	id := uuid.New()
	payload, _ := json.Marshal(event.TodoCreatedPayload{Title: "buy milk"})

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(pushBody(t, event.TypeTodoCreated, id, payload)))
	rec := httptest.NewRecorder()
	h.HandleEvent(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if rm.upserted[id] != "buy milk" {
		t.Fatalf("expected upsert of buy milk, got %v", rm.upserted)
	}
}

func TestHandleEvent_TodoCompleted(t *testing.T) {
	rm := newFakeRM()
	h := NewHandler(rm)
	id := uuid.New()
	payload, _ := json.Marshal(event.TodoCompletedPayload{})

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(pushBody(t, event.TypeTodoCompleted, id, payload)))
	rec := httptest.NewRecorder()
	h.HandleEvent(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if !rm.marked[id] {
		t.Fatalf("expected todo to be marked done")
	}
}

func TestHandleEvent_CompletedUnknownIDStillAcks(t *testing.T) {
	rm := newFakeRM()
	rm.markMatch = false // simulate no matching read row
	h := NewHandler(rm)
	id := uuid.New()
	payload, _ := json.Marshal(event.TodoCompletedPayload{})

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(pushBody(t, event.TypeTodoCompleted, id, payload)))
	rec := httptest.NewRecorder()
	h.HandleEvent(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 (unknown id should ack)", rec.Code)
	}
}

func TestHandleEvent_DBFailureRetries(t *testing.T) {
	rm := newFakeRM()
	rm.markErr = context.DeadlineExceeded
	h := NewHandler(rm)
	id := uuid.New()
	payload, _ := json.Marshal(event.TodoCompletedPayload{})

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(pushBody(t, event.TypeTodoCompleted, id, payload)))
	rec := httptest.NewRecorder()
	h.HandleEvent(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (DB failure should retry)", rec.Code)
	}
}

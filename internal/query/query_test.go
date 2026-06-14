package query

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/daadaamed/go-cqrs-pubsub/internal/store"
)

type fakeReadModel struct {
	list    []store.Todo
	get     *store.Todo
	listErr error
	getErr  error
}

func (f *fakeReadModel) ListTodos(_ context.Context) ([]store.Todo, error) {
	return f.list, f.listErr
}

func (f *fakeReadModel) GetTodo(_ context.Context, _ uuid.UUID) (*store.Todo, error) {
	return f.get, f.getErr
}

func TestListTodos(t *testing.T) {
	rm := &fakeReadModel{list: []store.Todo{
		{ID: uuid.New(), Title: "buy milk", Done: false},
	}}
	h := NewHandler(rm)

	req := httptest.NewRequest(http.MethodGet, "/todos", nil)
	rec := httptest.NewRecorder()
	h.ListTodos(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got []store.Todo
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].Title != "buy milk" {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestGetTodo(t *testing.T) {
	id := uuid.New()
	tests := []struct {
		name       string
		path       string
		rm         *fakeReadModel
		wantStatus int
	}{
		{"found", id.String(), &fakeReadModel{get: &store.Todo{ID: id, Title: "x"}}, http.StatusOK},
		{"not found", id.String(), &fakeReadModel{get: nil}, http.StatusNotFound},
		{"invalid id", "not-a-uuid", &fakeReadModel{}, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(tt.rm)
			req := httptest.NewRequest(http.MethodGet, "/todos/"+tt.path, nil)
			req.SetPathValue("id", tt.path)
			rec := httptest.NewRecorder()
			h.GetTodo(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}
}

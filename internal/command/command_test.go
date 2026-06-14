package command

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/daadaamed/go-cqrs-pubsub/internal/event"
)

type fakeAppender struct {
	got     *event.Event
	failErr error
}

func (f *fakeAppender) AppendEvent(_ context.Context, e *event.Event) error {
	if f.failErr != nil {
		return f.failErr
	}
	f.got = e
	return nil
}

type fakePublisher struct {
	published *event.Event
	failErr   error
}

func (f *fakePublisher) Publish(_ context.Context, e *event.Event) error {
	if f.failErr != nil {
		return f.failErr
	}
	f.published = e
	return nil
}

func TestCreateTodo(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantAppend bool
	}{
		{"valid", `{"title":"buy milk"}`, http.StatusCreated, true},
		{"trims whitespace", `{"title":"  buy milk  "}`, http.StatusCreated, true},
		{"empty title", `{"title":""}`, http.StatusBadRequest, false},
		{"whitespace only", `{"title":"   "}`, http.StatusBadRequest, false},
		{"missing title", `{}`, http.StatusBadRequest, false},
		{"malformed json", `{`, http.StatusBadRequest, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &fakeAppender{}
			pub := &fakePublisher{}
			h := NewHandler(app, pub)

			req := httptest.NewRequest(http.MethodPost, "/todos", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			h.CreateTodo(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if (app.got != nil) != tt.wantAppend {
				t.Fatalf("append happened = %v, want %v", app.got != nil, tt.wantAppend)
			}
			if tt.wantAppend {
				if app.got.Type != event.TypeTodoCreated {
					t.Errorf("event type = %q, want %q", app.got.Type, event.TypeTodoCreated)
				}
				if !strings.Contains(string(app.got.Payload), "buy milk") {
					t.Errorf("payload missing title: %s", app.got.Payload)
				}
				if pub.published == nil {
					t.Errorf("expected event to be published")
				}
			} else {
				if pub.published != nil {
					t.Errorf("invalid command should not publish")
				}
			}
		})
	}
}

func TestCreateTodo_PublishFailureStillSucceeds(t *testing.T) {
	app := &fakeAppender{}
	pub := &fakePublisher{failErr: context.DeadlineExceeded}
	h := NewHandler(app, pub)

	req := httptest.NewRequest(http.MethodPost, "/todos", strings.NewReader(`{"title":"x"}`))
	rec := httptest.NewRecorder()
	h.CreateTodo(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (publish failure must not fail command)", rec.Code)
	}
	if app.got == nil {
		t.Fatalf("event should still be appended")
	}
}

func TestCompleteTodo(t *testing.T) {
	id := uuid.New()
	tests := []struct {
		name       string
		pathID     string
		wantStatus int
		wantAppend bool
	}{
		{"valid", id.String(), http.StatusAccepted, true},
		{"invalid id", "not-a-uuid", http.StatusBadRequest, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &fakeAppender{}
			pub := &fakePublisher{}
			h := NewHandler(app, pub)

			req := httptest.NewRequest(http.MethodPost, "/todos/"+tt.pathID+"/complete", nil)
			req.SetPathValue("id", tt.pathID)
			rec := httptest.NewRecorder()
			h.CompleteTodo(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if (app.got != nil) != tt.wantAppend {
				t.Fatalf("append = %v, want %v", app.got != nil, tt.wantAppend)
			}
			if tt.wantAppend && app.got.Type != event.TypeTodoCompleted {
				t.Errorf("event type = %q, want %q", app.got.Type, event.TypeTodoCompleted)
			}
		})
	}
}

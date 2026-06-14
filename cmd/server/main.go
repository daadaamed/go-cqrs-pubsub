// Command server is the single Go service. Phase 2 closes the CQRS loop:
// POST /todos appends a TodoCreated event and publishes it; POST /events
// receives the Pub/Sub push and updates the read model.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daadaamed/go-cqrs-pubsub/internal/command"
	"github.com/daadaamed/go-cqrs-pubsub/internal/projection"
	"github.com/daadaamed/go-cqrs-pubsub/internal/pubsub"
	"github.com/daadaamed/go-cqrs-pubsub/internal/store"
)

type config struct {
	databaseURL  string
	port         string
	projectID    string
	topicID      string
	subscription string
	pushEndpoint string
}

func loadConfig() config {
	port := getenv("PORT", "8080")
	return config{
		databaseURL:  getenv("DATABASE_URL", "postgres://todo:todo@localhost:5432/todo?sslmode=disable"),
		port:         port,
		projectID:    getenv("PUBSUB_PROJECT_ID", "todo-local"),
		topicID:      getenv("PUBSUB_TOPIC", "todo-events"),
		subscription: getenv("PUBSUB_SUBSCRIPTION", "todo-projection"),
		pushEndpoint: getenv("PUSH_ENDPOINT", "http://host.docker.internal:"+port+"/events"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func run() error {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	ps, err := pubsub.New(ctx, pubsub.Config{
		ProjectID:    cfg.projectID,
		TopicID:      cfg.topicID,
		Subscription: cfg.subscription,
		PushEndpoint: cfg.pushEndpoint,
	})
	if err != nil {
		return err
	}
	defer ps.Close()
	log.Printf("pubsub ready: topic=%s sub=%s push=%s", cfg.topicID, cfg.subscription, cfg.pushEndpoint)

	cmd := command.NewHandler(st, ps)
	proj := projection.NewHandler(st)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /todos", cmd.CreateTodo)
	mux.HandleFunc("POST /events", proj.HandleEvent) // Pub/Sub push target
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:         ":" + cfg.port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on :%s", cfg.port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Println("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	log.Println("server stopped")
	return nil
}

// Command server is the single Go service. Phase 1 wires the command side only:
// POST /todos appends a TodoCreated event to the store. Pub/Sub, projection,
// and query endpoints arrive in later phases.
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

	"github.com/daadaamed/todo-cqrs/internal/command"
	"github.com/daadaamed/todo-cqrs/internal/store"
)

type config struct {
	databaseURL string
	port        string
}

func loadConfig() config {
	return config{
		databaseURL: getenv("DATABASE_URL", "postgres://todo:todo@localhost:5432/todo?sslmode=disable"),
		port:        getenv("PORT", "8080"),
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

	// Root context cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	cmd := command.NewHandler(st)

	mux := http.NewServeMux()
	// Go 1.22+ method+path patterns — no router dependency needed.
	mux.HandleFunc("POST /todos", cmd.CreateTodo)
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

	// Run the server until the context is cancelled.
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

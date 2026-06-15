package admin

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
)

// Rebuilder is the slice of the store this package needs (consumer-defined).
type Rebuilder interface {
	RebuildReadModel(ctx context.Context) (int, error)
}

// Handler serves admin endpoints.
type Handler struct {
	rb Rebuilder
}

// NewHandler wires the admin handler with its rebuild dependency.
func NewHandler(rb Rebuilder) *Handler {
	return &Handler{rb: rb}
}

// Rebuild handles POST /admin/rebuild: truncate and replay the event log into
// the read model. Returns the number of events applied.
func (h *Handler) Rebuild(w http.ResponseWriter, r *http.Request) {
	applied, err := h.rb.RebuildReadModel(r.Context())
	if err != nil {
		log.Printf("admin: rebuild: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "rebuild failed"})
		return
	}
	log.Printf("admin: rebuilt read model from %d events", applied)
	writeJSON(w, http.StatusOK, map[string]int{"events_applied": applied})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

CREATE TABLE events (
    id           BIGSERIAL   PRIMARY KEY,
    aggregate_id UUID        NOT NULL,
    type         TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_events_aggregate ON events (aggregate_id, id);
# Phase 0: infrastructure. Phase 1 adds app targets (run/build/test/tidy).
DATABASE_URL ?= postgres://todo:todo@localhost:5432/todo?sslmode=disable

.PHONY: up down logs migrate-up migrate-down psql ps run build test tidy

## Download/clean module dependencies.
tidy:
	go mod tidy

## Run the server (reads env from your shell; see .env.example).
run:
	go run ./cmd/server

## Build the binary into ./bin/server.
build:
	go build -o bin/server ./cmd/server

## Run unit tests.
test:
	go test ./...

## Bring up Postgres + Pub/Sub emulator and wait until healthy.
up:
	docker compose up -d
	@echo "waiting for services to be healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' todo-postgres)" = "healthy" ]; do sleep 1; done
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' todo-pubsub)" = "healthy" ]; do sleep 1; done
	@echo "infra ready."

## Stop and remove containers (keeps the pgdata volume).
down:
	docker compose down

## Tail logs from both services.
logs:
	docker compose logs -f

## Show container status.
ps:
	docker compose ps

## Apply migrations. Requires the golang-migrate CLI on PATH.
## Install: go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

## Roll back the most recent migration.
migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down 1

## Open a psql shell inside the Postgres container.
psql:
	docker exec -it todo-postgres psql -U todo -d todo
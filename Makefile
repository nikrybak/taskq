GO        ?= go
BIN       := bin/taskq
PKGS      := ./...
TEST_DATABASE_URL ?= postgres://taskq:taskq@localhost:5432/taskq_test?sslmode=disable

.PHONY: all build run migrate test test-integration lint vet staticcheck fmt tidy docker-up docker-down clean

all: lint test build

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/taskq

run:
	$(GO) run ./cmd/taskq serve

migrate:
	$(GO) run ./cmd/taskq migrate

test:
	$(GO) test -race -count=1 $(PKGS)

# Requires a reachable PostgreSQL; `make docker-up` starts one.
test-integration:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" $(GO) test -race -count=1 ./internal/repository/postgres/...

lint: vet staticcheck
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

vet:
	$(GO) vet $(PKGS)

staticcheck:
	$(GO) run honnef.co/go/tools/cmd/staticcheck@latest $(PKGS)

fmt:
	gofmt -w .

tidy:
	$(GO) mod tidy

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v

clean:
	rm -rf bin coverage.out

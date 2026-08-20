# Implementation Plan: Migrate Integration Testing to testcontainers-go

## Goal Description
Migrate the integration test suite from a manual `docker-compose` / GHA service setup to fully automated container management using [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go) (`modules/postgres`). 

Running `go test -tags=integration ./...` will automatically start an isolated PostgreSQL container via Testcontainers, run all integration test suites against it, and reliably tear down the container upon completion.

---

## User Review Required

> [!IMPORTANT]
> **Docker Daemon Prerequisite**: Running integration tests (`go test -tags=integration ./...` or `make test-integration`) will require a local Docker daemon running, but will no longer require pre-starting PostgreSQL via `docker compose` or port-binding to `5432`.

> [!NOTE]
> **Performance Optimization**: A single PostgreSQL test container is started once per package execution via `TestMain` in `postgres/integration_test/main_test.go`, keeping the total test suite runtime under ~3 seconds while preserving isolation between test cases via table teardown/migration replay.

---

## Proposed Changes

### Integration Test Suite (`postgres/integration_test`)

#### [NEW] [main_test.go](postgres/integration_test/main_test.go)
- Implement `TestMain(m *testing.M)` to start `postgres:16-alpine` via `github.com/testcontainers/testcontainers-go/modules/postgres`.
- Capture the dynamic connection string via `pgContainer.ConnectionString(ctx, "sslmode=disable")`.
- Terminate the container in teardown when `m.Run()` finishes.
- Define shared test helpers `testDB`, `getTestDB(t *testing.T)`, and `setupTestTables(t *testing.T, db testDB)` so that test files share standard setup logic without duplication.
- Preserve query execution mode configurations (`PGX_TEST_QUERY_EXEC_MODE` / `PGX_TEST_SIMPLE_PROTOCOL`).

```go
//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/eventsalsa/store/migrations"
)

var testDBConnStr string

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("eventsalsa_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatalf("failed to start postgres container: %v", err)
	}

	testDBConnStr, err = pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgContainer.Terminate(context.Background())
		log.Fatalf("failed to get connection string: %v", err)
	}

	code := m.Run()

	termCtx, termCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer termCancel()
	if err := pgContainer.Terminate(termCtx); err != nil {
		log.Printf("failed to terminate container: %v", err)
	}

	os.Exit(code)
}
```

#### [MODIFY] [store_test.go](postgres/integration_test/store_test.go)
- Remove duplicate definitions of `txOptions`, `testDB`, `getTestDB`, and `setupTestTables` (now centralized in `main_test.go`).
- Keep all test functions intact.

---

### Project Build & Automation

#### [DELETE] [docker-compose.yml](docker-compose.yml)
- Remove the `docker-compose.yml` file as it is replaced by Testcontainers.

#### [MODIFY] [Makefile](Makefile)
- Update `test-integration` target description to mention Testcontainers/Docker requirement.
- Remove obsolete `test-integration-local` target.

```makefile
test-integration: ## Run integration tests (uses testcontainers; requires Docker)
	go test -p 1 -v -tags=integration ./...
```

#### [MODIFY] [.github/workflows/ci.yml](.github/workflows/ci.yml)
- In the `integration-test` job:
  - Remove GitHub Actions `services: postgres:` container block.
  - Remove `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` environment variables.
  - Testcontainers connects directly to Docker on the runner.

---

### Dependencies

#### [MODIFY] [go.mod](go.mod) & `go.sum`
- Add `github.com/testcontainers/testcontainers-go` and `github.com/testcontainers/testcontainers-go/modules/postgres`.
- Run `go mod tidy` to clean up and verify checksums.

---

### Documentation

#### [MODIFY] [README.md](README.md)
- Update development instructions: replace `make test-integration-local` and manual `docker compose up -d` instructions with `make test-integration` / `go test -p 1 -v -tags=integration ./...`.

#### [MODIFY] [AGENTS.md](AGENTS.md)
- Update testing guidelines: note that integration tests use Testcontainers and require Docker running.

---

## Verification Plan

### Automated Tests
1. **Module & Dependency Verification**:
   ```bash
   go mod tidy
   go mod verify
   ```
2. **Unit Tests**:
   ```bash
   go test -v -race ./...
   ```
3. **Integration Tests with Testcontainers**:
   ```bash
   go test -p 1 -v -tags=integration ./...
   ```
   Confirm all tests in `postgres/integration_test` (e.g. `TestAppendEvents`, `TestAppendEvents_OptimisticConcurrency`, `TestReadEvents`, `TestExpectedVersion_*`) pass and dynamically start/stop the Postgres container.
4. **Linting**:
   ```bash
   golangci-lint run --timeout=5m
   ```
5. **Makefile Target Verification**:
   ```bash
   make test-integration
   ```

# Walkthrough: Migrate Integration Tests to testcontainers-go

We have migrated the PostgreSQL integration test suite from manual `docker-compose` management to automated lifecycle management with [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go) (`modules/postgres`).

## Key Changes

### 1. Testcontainers Test Harness
- **[main_test.go](postgres/integration_test/main_test.go)**:
  - Added `TestMain` to start a single `postgres:16-alpine` container per test package execution using `github.com/testcontainers/testcontainers-go/modules/postgres`.
  - Configured dynamic connection string retrieval via `pgContainer.ConnectionString(ctx, "sslmode=disable")`.
  - Added container termination in `TestMain` cleanup.
  - Centralized shared test helpers (`testDB`, `txOptions`, `getTestDB`, and `setupTestTables`) across integration tests.
  - Retained support for custom query execution mode flags (`PGX_TEST_QUERY_EXEC_MODE` / `PGX_TEST_SIMPLE_PROTOCOL`).

### 2. Test Suite Cleanup
- **[store_test.go](postgres/integration_test/store_test.go)**:
  - Removed duplicated helper types and connection logic in favor of the shared implementation in `main_test.go`.

### 3. Build & CI Configuration
- **[docker-compose.yml](docker-compose.yml)**: Removed obsolete compose file.
- **[Makefile](Makefile)**:
  - Removed `test-integration-local` target.
  - Updated `test-integration` description.
- **[.github/workflows/ci.yml](.github/workflows/ci.yml)**:
  - Removed GitHub Actions `services: postgres:` service container block and host environment variables. Tests run directly against the runner's Docker daemon via Testcontainers.

### 4. Dependencies & Documentation
- **[go.mod](go.mod)** / **`go.sum`**: Added `github.com/testcontainers/testcontainers-go` and `github.com/testcontainers/testcontainers-go/modules/postgres`.
- **[README.md](README.md)** & **[AGENTS.md](AGENTS.md)**: Updated development instructions to reflect Testcontainers usage.

---

## Verification Results

### Automated Tests & Linting
- **Unit Tests**:
  ```bash
  go test -v -race ./...
  ```
  Result: All unit tests passed across all packages.
- **Integration Tests (Testcontainers)**:
  ```bash
  go test -p 1 -v -tags=integration ./...
  ```
  Result: Successfully booted `postgres:16-alpine`, executed all 20 integration tests (including optimistic concurrency, version tracking, stream reads, and gaps), and cleaned up container.
- **Makefile Target**:
  ```bash
  make test-integration
  ```
  Result: Passed.
- **Linter**:
  ```bash
  golangci-lint run --timeout=5m
  ```
  Result: 0 issues.

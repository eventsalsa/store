# Walkthrough: Migrate Integration Tests to testcontainers-go

We have migrated the PostgreSQL integration test suite from manual `docker-compose` management to automated lifecycle management with [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go) (`modules/postgres`). In addition, `pgx` was upgraded to `v5.10.0` and unit tests now run across a Go matrix of `1.25`, `1.26`, and `1.27`.

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
  - Set `GOTOOLCHAIN: local` to prevent toolchain switching issues.
  - Configured unit test matrix across Go versions `1.25`, `1.26`, and `1.27`.

### 4. Dependencies & Documentation
- **[go.mod](go.mod)** / **`go.sum`**:
  - Upgraded `github.com/jackc/pgx/v5` to `v5.10.0`.
  - Added `github.com/testcontainers/testcontainers-go` (`v0.44.0`) and `github.com/testcontainers/testcontainers-go/modules/postgres` (`v0.44.0`).
- **[README.md](README.md)** & **[AGENTS.md](AGENTS.md)**: Updated development instructions to reflect Testcontainers usage.

---

## Verification Results

### CI Workflow (`CI #32412418818`)
- **Unit Tests (Go 1.25)**: Passed (49s)
- **Unit Tests (Go 1.26)**: Passed (1m6s)
- **Unit Tests (Go 1.27)**: Passed (20s)
- **Integration Tests (Testcontainers)**: Passed (1m14s)
- **Lint**: Passed (40s)
- **Build**: Passed (24s)

---
name: go-testing
description: Guidelines for writing unit and integration tests in Go, covering test design, subtests, concurrency, and cleanups.
---

# Go Testing & Quality Assurance

This skill defines the practices for writing comprehensive and correct tests in Go for this library.

## How to Write Tests

### 1. Table-Driven Tests
*   Use map-based or slice-based table-driven tests: `map[string]struct{ ... }` or `[]struct{ name string; ... }`.
*   Descriptive case names: Describe the scenario, not the expected technical output (e.g., `"append events when stream exists"` instead of `"returns error"`).
*   Structure: Use the Arrange-Act-Assert pattern clearly.

### 2. What to Test
*   **Happy Paths**: Test standard, expected operation with valid inputs.
*   **Error Paths**: Every error return path must be exercised by at least one test case.
*   **Boundary Conditions**: Test empty slices, zero values, nil inputs, and extremes.
*   **Concurrency**: For concurrent features, verify behavior under load using multiple goroutines and `sync.WaitGroup`.
*   **Idempotency**: Verify that executing identical operations twice does not produce side effects when idempotency is expected.

### 3. Conventions
*   **Unit Tests**: Co-locate unit tests with production code using the `*_test.go` filename convention.
*   **Integration Tests**:
    *   Require a live PostgreSQL instance.
    *   Must include the `//go:build integration` build tag.
    *   Leverage environment variables for database credentials: `POSTGRES_HOST`, `POSTGRES_PORT`, etc.
*   **Assertions**: Use standard `testing.T` APIs instead of external assertion libraries like `testify`.
*   **Subtests**: Use `t.Run(name, func(t *testing.T) { ... })` for table-driven cases.
*   **Cleanup**: Register resource teardown using `t.Cleanup()`.

## Commands

```bash
# Run unit tests
go test ./...

# Run unit tests with race detection and coverage
go test -v -race -coverprofile=coverage.out ./...

# Run integration tests (requires PostgreSQL)
go test -p 1 -v -tags=integration ./...
```

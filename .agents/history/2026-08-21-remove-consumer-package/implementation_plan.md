# Implementation Plan: Remove Consumer Package

## Background & Rationale
The `consumer` package contained `Consumer` and `ScopedConsumer` interface declarations. These interfaces lacked an in-box runner/worker implementation in this repository and leaked database driver types (`pgx.Tx`) into consumer contracts. Handler and execution contracts belong in downstream consumer runners and projection engines rather than the core storage library. Since the library is in `0.x` pre-major development with `"bump-minor-pre-major": true` in Release Please, removing these interfaces cleans up the API surface without triggering a `1.0.0` major release.

## Proposed Changes
1. **Remove `consumer/` package**: Delete `consumer/consumer.go` and `consumer/consumer_test.go`.
2. **Update `README.md`**: Remove the `Consumer interfaces` feature list item, remove the `### Consumers` section, and ensure no references to `Consumer` or `ScopedConsumer` remain.
3. **Update `AGENTS.md`**: Update core concepts and package structure tree.
4. **Update `eventmap` generator**: Adjust doc comment in `eventmap/generator.go` and generated example code.
5. **Verify**: Run unit tests, integration tests with Testcontainers, and linter.

## Verification Plan
- Unit tests: `go test ./...`
- Integration tests: `go test -p 1 -v -tags=integration ./...`
- Lint: `golangci-lint run`

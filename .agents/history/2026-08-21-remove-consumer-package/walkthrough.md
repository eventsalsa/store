# Walkthrough: Remove Consumer Package

We have removed the `consumer` package containing the `Consumer` and `ScopedConsumer` interface definitions.

## Rationale
- **No in-box runner/runtime**: The `consumer` package contained only interface contracts without an execution runtime or worker engine in this repository.
- **Database driver coupling**: The `Consumer.Handle` method accepted `pgx.Tx`, leaking PostgreSQL-specific transaction types into general consumer handler definitions.
- **Clear separation of concerns**: Event consuming and projection contracts belong in downstream consumer runners and projection engines rather than the core storage library.

## Key Changes

### 1. Consumer Package Removal
- Deleted `consumer/consumer.go` and `consumer/consumer_test.go`.

### 2. Documentation Updates
- **[README.md](README.md)**:
  - Removed `Consumer interfaces` from the feature highlights.
  - Removed the `### Consumers` section and code snippets.
  - Updated notification subscriber wording in the `pg_notify` section.
- **[AGENTS.md](AGENTS.md)**:
  - Removed consumer concepts from the core concepts.
  - Removed `consumer/` from the package directory tree.

### 3. Code Generation & Examples
- **[eventmap/generator.go](eventmap/generator.go)** & **[examples/eventmap-codegen/infrastructure/persistence/event_mapping.gen.go](examples/eventmap-codegen/infrastructure/persistence/event_mapping.gen.go)**:
  - Updated doc comment for `FromESEvent` to reference generic event and projection handlers.

---

## Verification Results

### Unit Tests
```bash
go test -count=1 ./...
```
- `github.com/eventsalsa/store`: PASS
- `github.com/eventsalsa/store/eventmap`: PASS
- `github.com/eventsalsa/store/examples/eventmap-codegen/infrastructure/persistence`: PASS
- `github.com/eventsalsa/store/migrations`: PASS
- `github.com/eventsalsa/store/postgres`: PASS

### Integration Tests (Testcontainers PostgreSQL 16)
```bash
go test -p 1 -v -tags=integration ./...
```
- All concurrent append, expected version, NOTIFY, and range partitioning tests passed.

### Linting
```bash
golangci-lint run
```
- `0 issues.`

# GitHub Copilot Instructions for eventsalsa/store

## Project Overview

`github.com/eventsalsa/store` is a professional-grade event store library for Go, designed with clean
architecture principles. It provides minimal, production-ready infrastructure for persisting and reading
domain events in an event-sourced system. State changes are stored as an ordered, immutable sequence of
events in a PostgreSQL database.

## Architecture & Design Principles

### Core Concepts

- **Clean Architecture**: Core interfaces are defined in the root `store` package, independent of any
  specific database driver
- **Caller-Controlled Transactions**: All store methods accept `*sql.Tx` directly — callers begin
  transactions and control commit/rollback boundaries; the library never commits or rolls back
- **Optimistic Concurrency**: Built-in version conflict detection via database constraints and the
  `aggregate_heads` table
- **Immutable Events**: `Event` is a value object before persistence; `PersistedEvent` is the
  immutable record returned after storage
- **Pull-Based Consumers**: Sequential event processing by global position using named, checkpointed
  consumers

### Package Structure

```
store/                     # Core types (Event, PersistedEvent, Stream, AppendResult)
│                          # + store interfaces (EventStore, EventReader, AggregateStreamReader,
│                          #   GlobalPositionReader) + expected version helpers
├── consumer/              # Consumer and ScopedConsumer interfaces
├── postgres/              # PostgreSQL implementation of all store interfaces
├── migrations/            # SQL migration generator (events + aggregate_heads tables)
├── eventmap/              # Code generator: maps domain event structs ↔ store.Event / store.PersistedEvent
├── cmd/
│   ├── migrate-gen/       # CLI tool: generates SQL migration files
│   └── eventmap-gen/      # CLI tool: generates event mapping code
```

## Development Guidelines

### Language & Versions

- **Go Version**: 1.23 or later (go.mod specifies 1.24.11)
- **PostgreSQL**: Version 12+ (16 used for integration tests)
- **Dependencies**: Minimal — `github.com/google/uuid` and `github.com/lib/pq`

### Code Style & Conventions

1. **Interface Design**
   - Keep interfaces minimal and focused on single responsibilities
   - All database operations accept `context.Context` as the first parameter
   - All database operations accept `*sql.Tx` as the second parameter — never `*sql.DB`
   - Define interfaces before implementations; keep them in the root `store` package

2. **Error Handling**
   - Return clear, specific sentinel errors (e.g., `store.ErrOptimisticConcurrency`, `store.ErrNoEvents`)
   - Wrap errors with context: `fmt.Errorf("failed to append events: %w", err)`
   - Always check and propagate errors; never silently discard them

3. **Naming Conventions**
   - Event types: descriptive past-tense nouns (e.g., `UserCreated`, `OrderPlaced`)
   - Aggregate types: singular nouns (e.g., `User`, `Order`)
   - Package names: short, lowercase, no underscores
   - Config structs: `XxxConfig` with a `DefaultXxxConfig()` constructor

4. **Concurrency**
   - Use optimistic concurrency via `ExpectedVersion` (`Any()`, `NoStream()`, `Exact(N)`)
   - The `aggregate_heads` table provides O(1) current-version lookups
   - Version conflicts return `store.ErrOptimisticConcurrency` — callers should retry

5. **Database Schema**
   - `BYTEA` for event `payload` and `metadata` (format-agnostic)
   - `BIGSERIAL` for `global_position` (globally ordered event log)
   - `UUID` for `event_id` and `aggregate_id`
   - `aggregate_heads` table for efficient version tracking (avoids full-table scans)
   - Unique constraint on `(aggregate_type, aggregate_id, aggregate_version)` as the last
     concurrency safety net

6. **Logger**
   - Logger is optional in config structs; always guard with `if config.Logger != nil`

7. **Line Length**: 120 characters max

### Testing Practices

#### Unit Tests

- Test files named `*_test.go`, co-located with the package under test
- Table-driven tests for comprehensive coverage
- Run with standard `go test ./...`

#### Integration Tests

- Require a live PostgreSQL instance
- Build tag: `//go:build integration`
- Set environment variables: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`,
  `POSTGRES_PASSWORD`, `POSTGRES_DB`
- Start PostgreSQL for local runs:
  ```bash
  docker run -d -p 5432:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=eventsalsa_test \
    postgres:16
  ```

#### Test Commands

```bash
# Unit tests only
go test ./...

# Unit tests with race detection and coverage
go test -v -race -coverprofile=coverage.out ./...

# Integration tests (requires PostgreSQL)
go test -p 1 -v -tags=integration ./...

# Integration tests local (starts PostgreSQL via Docker Compose)
make test-integration-local
```

### Linting & Code Quality

- **Linter**: golangci-lint (version 2 config in `.golangci.yml`)
- **Command**: `golangci-lint run` or `golangci-lint run --timeout=5m`
- **Auto-fix**: Use `golangci-lint run --fix` to automatically fix simple issues
- **Workflow**: Always run the linter after making code changes, and use `--fix` for fixable issues
- **MANDATORY**: After fixing any linting issues, **ALWAYS** run the linter globally
  (`golangci-lint run --timeout=5m`) to ensure no new issues were introduced elsewhere
- **Enabled Linters**: gocritic, gocyclo, gosec, misspell, revive
- **Formatters**: gofmt, goimports (with local prefix `github.com/eventsalsa/store`)
- **Complexity**: Maximum cyclomatic complexity of 15
- **Line Length**: 120 characters max
- **Exclusions**: Test files, examples, and third-party code have relaxed rules

### Building

```bash
# Build all packages
go build -v ./...

# Download dependencies
go mod download

# Verify dependencies
go mod verify

# Generate migrations
go run github.com/eventsalsa/store/cmd/migrate-gen -output migrations

# Generate event mapping code
go run github.com/eventsalsa/store/cmd/eventmap-gen -input ./events -output ./eventmap_gen.go
```

### Git Commits

- Use a Conventional Commits subject line for every commit (for example, `feat: add postgres event store`)
- Every commit message must include a blank line followed by a fuller body that explains the main
  changes and why they were made
- Never create a subject-only commit

### Migration Generation

Use the `migrate-gen` tool to create database migrations:

```bash
go run github.com/eventsalsa/store/cmd/migrate-gen -output migrations
```

Or add a `go:generate` directive:

```go
//go:generate go run github.com/eventsalsa/store/cmd/migrate-gen -output ../../migrations
```

This generates SQL that creates:

- `events` table with proper indexes and the `global_position` sequence
- `aggregate_heads` table for O(1) version lookups
- All necessary unique constraints for optimistic concurrency

## CI/CD

The repository uses GitHub Actions with the following jobs:

1. **Unit Tests** — Runs on Go 1.23, 1.24, 1.25 with race detection
2. **Integration Tests** — Runs on Go 1.25 with a PostgreSQL 16 service container
3. **Lint** — Runs golangci-lint with a 5-minute timeout
4. **Build** — Verifies all packages compile successfully

All jobs run on pull requests and pushes to the `master` branch.

## Common Patterns

### Appending Events

```go
// Build events — AggregateVersion and GlobalPosition are assigned by the store
events := []store.Event{
    {
        AggregateType: "User",
        AggregateID:   userID.String(),
        EventID:       uuid.New(),
        EventType:     "UserCreated",
        EventVersion:  1,
        Payload:       []byte(`{"email":"user@example.com"}`),
        Metadata:      []byte(`{}`),
        CreatedAt:     time.Now(),
    },
}

// Caller controls the transaction boundary
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}

// NoStream() enforces the aggregate must not already exist
result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
if err != nil {
    tx.Rollback()
    return err
}

if err := tx.Commit(); err != nil {
    return err
}

// Inspect what was persisted
newVersion := result.ToVersion()
```

### Expected Version Variants

```go
// Aggregate must not exist (creation commands)
result, err := eventStore.Append(ctx, tx, store.NoStream(), events)

// Aggregate must be at a specific version (update commands with optimistic concurrency)
result, err := eventStore.Append(ctx, tx, store.Exact(currentVersion), events)

// No version check (use sparingly — bypasses optimistic concurrency)
result, err := eventStore.Append(ctx, tx, store.Any(), events)
```

### Reading Aggregate Streams

```go
// Read all events for an aggregate
stream, err := eventStore.ReadAggregateStream(ctx, tx, "User", aggregateID, nil, nil)
if err != nil {
    return err
}

if stream.IsEmpty() {
    // Aggregate does not exist
}

currentVersion := stream.Version()

for _, event := range stream.Events {
    // Reconstruct aggregate state from event
}

// Read from a specific version onwards (e.g., after a snapshot)
fromVersion := int64(5)
stream, err = eventStore.ReadAggregateStream(ctx, tx, "User", aggregateID, &fromVersion, nil)

// Read a bounded version range
toVersion := int64(10)
stream, err = eventStore.ReadAggregateStream(ctx, tx, "User", aggregateID, &fromVersion, &toVersion)
```

### Reading Events Sequentially

```go
// Read a batch of events from a known global position
events, err := eventStore.ReadEvents(ctx, tx, fromPosition, 100)
if err != nil {
    return err
}

for _, event := range events {
    // Process event; advance checkpoint to event.GlobalPosition
}
```

### Implementing a Consumer

```go
// Consumer receives all events
type AuditLogConsumer struct{}

func (c *AuditLogConsumer) Name() string { return "audit_log" }

func (c *AuditLogConsumer) Handle(ctx context.Context, tx *sql.Tx, event store.PersistedEvent) error {
    // tx is the caller's transaction — use it for atomic read model + checkpoint updates.
    // For non-SQL destinations (Kafka, Elasticsearch, Redis), ignore tx and use your own client.
    // NEVER call tx.Commit() or tx.Rollback() — the infrastructure that invoked Handle owns the transaction.
    _, err := tx.ExecContext(ctx,
        `INSERT INTO audit_log (event_id, event_type, aggregate_id, created_at)
         VALUES ($1, $2, $3, $4)`,
        event.EventID, event.EventType, event.AggregateID, event.CreatedAt,
    )
    return err
}
```

### Implementing a Scoped Consumer

```go
// ScopedConsumer receives only events for specific aggregate types
type UserReadModelConsumer struct{}

func (c *UserReadModelConsumer) Name() string { return "user_read_model" }

// AggregateTypes filters to User events only — other aggregate types are skipped
func (c *UserReadModelConsumer) AggregateTypes() []string { return []string{"User"} }

func (c *UserReadModelConsumer) Handle(ctx context.Context, tx *sql.Tx, event store.PersistedEvent) error {
    switch event.EventType {
    case "UserCreated":
        // Upsert read model using tx
    case "UserEmailChanged":
        // Update email in read model using tx
    }
    return nil
}
```

## Best Practices for Contributors

1. **Zero Dependencies**: Avoid adding external dependencies unless absolutely necessary
2. **Clean Interfaces**: Keep interfaces minimal and focused on single responsibilities
3. **Caller-Controlled Transactions**: Never commit or rollback `*sql.Tx` inside library code — callers
   control all transaction boundaries
4. **Immutability**: Events are value objects; never mutate a `PersistedEvent` after it is returned
5. **Version Assignment**: Let the store assign `AggregateVersion` and `GlobalPosition` automatically
   during `Append` — callers must not set these fields
6. **Testing**: Provide both unit tests and integration tests for new features
7. **Documentation**: Update README.md examples when adding new features or changing existing APIs
8. **Security**: Run `gosec` as part of linting to catch security issues early

## Security Considerations

- Use parameterized queries everywhere to prevent SQL injection
- Validate input data before persisting events
- Treat `Payload` and `Metadata` as opaque bytes — be cautious about logging their contents
- Apply the principle of least privilege to PostgreSQL roles used in production

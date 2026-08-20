# Agent Guidelines for eventsalsa/store

This document defines the architecture, conventions, workflows, and skills used by AI agents working on this project.

## Project Overview & Architecture

`github.com/eventsalsa/store` is a professional-grade event store library for Go, designed with clean architecture principles. It provides minimal, production-ready infrastructure for persisting and reading domain events in an event-sourced system. State changes are stored as an ordered, immutable sequence of events in a PostgreSQL database.

### Core Concepts

*   **Clean Architecture**: Core interfaces are defined in the root `store` package, independent of any specific database driver.
*   **Caller-Controlled Transactions**: All store methods accept `pgx.Tx` directly. Callers begin transactions and control commit/rollback boundaries; the library never commits or rolls back.
*   **Optimistic Concurrency**: Built-in version conflict detection via database constraints and the `stream_heads` table.
*   **Immutable Events**: `Event` is a value object before persistence; `PersistedEvent` is the immutable record returned after storage.
*   **Pull-Based Consumers**: Sequential event processing by global position using named, checkpointed consumers.

### Package Structure

```
store/                     # Core types (Event, PersistedEvent, Stream, AppendResult)
│                          # + store interfaces (EventStore, EventReader, StreamReader,
│                          #   GlobalPositionReader) + expected version helpers
├── consumer/              # Consumer and ScopedConsumer interfaces
├── postgres/              # PostgreSQL implementation of all store interfaces
├── migrations/            # SQL migration generator (events + stream_heads tables)
├── eventmap/              # Code generator: maps domain event structs ↔ store.Event / store.PersistedEvent
├── cmd/
│   ├── migrate-gen/       # CLI tool: generates SQL migration files
│   └── eventmap-gen/      # CLI tool: generates event mapping code
```

---

## Testing & Verification

*   **Unit Tests**: `go test ./...`
*   **Integration Tests** (requires PostgreSQL container): `go test -p 1 -v -tags=integration ./...`
*   **Linting**: `golangci-lint run`
*   **Conventions**: Use standard Go `testing.T` assertions without third-party test assertion libraries. Keep integration tests isolated with `//go:build integration`.

---

## Git Conventions

All agents and contributors must adhere to strict git guidelines.

### 1. Branch Naming
Create branches using conventional prefixes:
*   `feat/` for new features (e.g., `feat/partitioned-event-store`)
*   `fix/` for bug fixes (e.g., `fix/optimistic-concurrency`)
*   `chore/` for maintenance and tooling tasks (e.g., `chore/convert-copilot-to-agent-skills`)
*   `refactor/` for code refactoring with no behavior changes

### 2. Conventional Commits
All commits must follow the Conventional Commits specification:
```
type(scope): subject line description
```

### 3. Multi-Line Commits
Commit messages must include a subject line and a detailed body explaining what changed and why.
*   **Crucial Rule**: Always construct multi-line commits using multiple `-m` arguments in the `git commit` command rather than using literal newlines (`\n`) in a single `-m` string.
*   **Example Command**:
    ```bash
    git commit -m "feat(postgres): add partitioned event store" -m "Introduce stream partitioning support using PostgreSQL declarative partitioning." -m "This optimizes query performance for very large event logs and allows archival partition-swapping."
    ```
    This method guarantees clean commit histories across all operating systems and shells.

---

## Common Patterns

### Appending Events

```go
// Build events — StreamVersion and GlobalPosition are assigned by the store
events := []store.Event{
    {
        StreamType:   "User",
        StreamID:     userID.String(),
        EventID:      uuid.New(),
        EventType:    "UserCreated",
        EventVersion: 1,
        Payload:      []byte(`{"email":"user@example.com"}`),
        Metadata:     []byte(`{}`),
        CreatedAt:    time.Now(),
    },
}

// Caller controls the transaction boundary
tx, err := db.Begin(ctx)
if err != nil {
    return err
}

// NoStream() enforces the stream must not already exist
result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
if err != nil {
    tx.Rollback(ctx)
    return err
}

if err := tx.Commit(ctx); err != nil {
    return err
}

// Inspect what was persisted
newVersion := result.ToVersion()
```

### Expected Version Variants

```go
// Stream must not exist (creation commands)
result, err := eventStore.Append(ctx, tx, store.NoStream(), events)

// Stream must be at a specific version (update commands with optimistic concurrency)
result, err := eventStore.Append(ctx, tx, store.Exact(currentVersion), events)

// No version check (use sparingly — bypasses optimistic concurrency)
result, err := eventStore.Append(ctx, tx, store.Any(), events)
```

### Reading Streams

```go
// Read all events for a stream
stream, err := eventStore.ReadStream(ctx, tx, "User", streamID, nil, nil)
if err != nil {
    return err
}

if stream.IsEmpty() {
    // Stream does not exist
}

currentVersion := stream.Version()

for _, event := range stream.Events {
    // Reconstruct stream state from event
}
```

# eventsalsa/store

[![CI](https://github.com/eventsalsa/store/actions/workflows/ci.yml/badge.svg)](https://github.com/eventsalsa/store/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/eventsalsa/store)](https://goreportcard.com/report/github.com/eventsalsa/store)
[![GoDoc](https://godoc.org/github.com/eventsalsa/store?status.svg)](https://godoc.org/github.com/eventsalsa/store)

A minimal, production-ready event store for Go.

## Features

- **PostgreSQL-backed event store** — append-only, immutable event log with BIGSERIAL global positions
- **Optimistic concurrency control** — via expected versions enforced at the database level via `stream_heads`
- **Optional range partitioning** — native PostgreSQL declarative RANGE partitioning & `pg_partman` integration
- **Stream reads** — load a full or partial event history with optional version ranges
- **Sequential event reading** — read events by global position for building consumers and projections
- **Transaction-first design** — all operations accept `pgx.Tx`; you control transaction boundaries
- **Consumer interfaces** — `Consumer` and `ScopedConsumer` for event processing
- **SQL migration generator** — `cmd/migrate-gen` generates a ready-to-apply `.sql` file
- **Event mapping code generator** — `cmd/eventmap-gen` generates type-safe domain event mappings


## Quick Start

### 1. Install

```bash
go get github.com/eventsalsa/store
```

Choose your PostgreSQL driver:

```bash
go get github.com/jackc/pgx/v5
```

### 2. Generate Migrations

```bash
go run github.com/eventsalsa/store/cmd/migrate-gen -output migrations
```

Apply the generated file with your preferred migration tool:

```bash
psql -h localhost -U postgres -d mydb -f migrations/*_init_event_sourcing.sql
```

### 3. Append and Read Events

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"

    "github.com/google/uuid"
    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/eventsalsa/store"
    "github.com/eventsalsa/store/postgres"
)

func main() {
    ctx := context.Background()
    db, err := pgxpool.New(ctx, "host=localhost user=postgres dbname=mydb sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    s := postgres.NewStore(postgres.DefaultStoreConfig())

    // Append events to a new stream
    userID := uuid.New().String()
    payload, _ := json.Marshal(map[string]string{"email": "alice@example.com", "name": "Alice"})

    tx, err := db.Begin(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer tx.Rollback(ctx) //nolint:errcheck

    result, err := s.Append(ctx, tx, store.NoStream(), []store.Event{
        {
            StreamType:   "User",
            StreamID:     userID,
            EventID:      uuid.New(),
            EventType:    "UserCreated",
            EventVersion: 1,
            Payload:      payload,
            Metadata:     []byte(`{}`),
            CreatedAt:    time.Now(),
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    if err := tx.Commit(ctx); err != nil {
        log.Fatal(err)
    }

    log.Printf("appended at positions %v, stream now at version %d",
        result.GlobalPositions, result.ToVersion())

    // Read the stream
    tx2, _ := db.Begin(ctx)
    defer tx2.Rollback(ctx) //nolint:errcheck

    stream, err := s.ReadStream(ctx, tx2, "User", userID, nil, nil)
    if err != nil {
        log.Fatal(err)
    }
    tx2.Commit(ctx) //nolint:errcheck

    log.Printf("stream: %d events, current version %d", stream.Len(), stream.Version())
    for _, e := range stream.Events {
        log.Printf("  v%d  %s  pos=%d", e.StreamVersion, e.EventType, e.GlobalPosition)
    }
}
```

## Core Concepts

### Events & Streams

`store.Event` is an immutable value object that you construct before persisting. The store assigns `StreamVersion` and `GlobalPosition` during `Append`.

```go
event := store.Event{
    StreamType:    "Order",         // logical category of the stream
    StreamID:      orderID,         // string identifier — UUID, email, slug, etc.
    EventID:       uuid.New(),      // idempotency key for the event itself
    EventType:     "OrderPlaced",   // discriminator used by consumers
    EventVersion:  1,               // schema version of the payload
    Payload:       payload,         // serialized domain data (JSON, proto, etc.)
    Metadata:      metadata,        // cross-cutting concerns (request ID, actor, etc.)
    CreatedAt:     time.Now(),
    // optional tracing fields:
    TraceID:       store.NullString{String: traceID, Valid: true},
    CorrelationID: store.NullString{String: corrID, Valid: true},
    CausationID:   store.NullString{String: causID, Valid: true},
}
```

`store.PersistedEvent` is what you read back. It adds `GlobalPosition` and `StreamVersion`.

`store.Stream` wraps the full ordered history of a single stream along with helper methods:

```go
stream.Version()  // current stream version (0 if empty)
stream.IsEmpty()  // true if no events were found
stream.Len()      // number of events in the stream
```

`store.AppendResult` describes the outcome of a write:

```go
result.ToVersion()       // stream version after the append
result.FromVersion()     // stream version before the append
result.GlobalPositions   // global positions assigned to each event
result.Events            // persisted events with all fields populated
```

### Expected Versions

Expected versions are the mechanism for optimistic concurrency. You declare the state you expect the stream to be in before writing.

| Constructor | When to use |
|---|---|
| `store.NoStream()` | Creating a new stream — fails if it already exists |
| `store.Exact(n)` | Updating an existing stream at a known version — fails on conflict |
| `store.Any()` | Unconditional write — skips version validation entirely |

Conflicts return `store.ErrOptimisticConcurrency`. The database unique constraint on `(stream_type, stream_id, stream_version)` acts as a final safety net even if two transactions pass the application-level check simultaneously.

```go
// Create — must not already exist
result, err := s.Append(ctx, tx, store.NoStream(), events)

// Update at a known version
result, err := s.Append(ctx, tx, store.Exact(stream.Version()), events)

// Unconditional
result, err := s.Append(ctx, tx, store.Any(), events)

if errors.Is(err, store.ErrOptimisticConcurrency) {
    // reload, reapply command, retry
}
```

### Stream Reads

`ReadStream` returns the ordered event history for a single stream instance. Both version bounds are optional and inclusive.

```go
// Full history
stream, err := s.ReadStream(ctx, tx, "User", userID, nil, nil)

// From a specific version onwards (e.g., to skip already-processed events)
from := int64(5)
stream, err = s.ReadStream(ctx, tx, "User", userID, &from, nil)

// A version window
from, to := int64(5), int64(10)
stream, err = s.ReadStream(ctx, tx, "User", userID, &from, &to)
```

### Sequential Reads

`ReadEvents` reads from the raw global log in position order, which is the basis for building consumers
and projections.

Because `global_position` is sequence-backed, these positions are unique and sortable but not a safe
naive checkpoint frontier under concurrent writers. Async consumers that persist checkpoints should use a
gap-aware worker/runtime rather than blindly advancing to the highest seen position.

```go
// Read up to 500 events after global position 0
events, err := s.ReadEvents(ctx, tx, 0, 500)

// Continue from last processed position
events, err = s.ReadEvents(ctx, tx, lastPosition, 500)
```

`GetLatestGlobalPosition` returns the highest position currently visible in the log — useful for
lightweight wakeup or polling checks without fetching full batches. It is not a safe contiguous
high-water mark for checkpoint advancement under concurrent writers.

```go
latest, err := s.GetLatestGlobalPosition(ctx, tx)
```

> **Checkpoint safety under concurrent writers:** `global_position` is backed by a PostgreSQL
> `BIGSERIAL` sequence, which guarantees uniqueness but not commit order. Under concurrent
> writers, a lower position may become visible after a higher one has already been returned.
> Advancing a checkpoint to the highest seen position without accounting for in-flight gaps
> can permanently skip events. Async consumers must use a gap-aware worker or runtime — do
> not treat the highest position returned by `ReadEvents` or `GetLatestGlobalPosition`
> as a safe naive checkpoint frontier under concurrent writers.

Scoped async filtering is intentionally a worker/runtime concern rather than a store read primitive.
If a consumer needs to react to only some stream types, establish a safe frontier from the unscoped
global stream first, then filter inside the runtime.

### Consumers

The `consumer` package defines the interfaces for event processing.

`consumer.Consumer` is the base interface:

```go
type AuditLogConsumer struct{}

func (c *AuditLogConsumer) Name() string { return "audit_log.v1" }

func (c *AuditLogConsumer) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
    // tx is the processor's transaction — use it for atomic read model + checkpoint updates.
    // Never call tx.Commit() or tx.Rollback() here; the processor owns that.
    _, err := tx.Exec(ctx,
        "INSERT INTO audit_log (event_id, event_type, occurred_at) VALUES ($1, $2, $3)",
        event.EventID, event.EventType, event.CreatedAt,
    )
    return err
}
```

`consumer.ScopedConsumer` narrows delivery to specific stream types. Consumers that implement only `Consumer` receive all events.

```go
type UserReadModel struct{}

func (p *UserReadModel) Name() string            { return "user_read_model.v1" }
func (p *UserReadModel) StreamTypes() []string   { return []string{"User"} }

func (p *UserReadModel) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
    // Only receives events where StreamType == "User"
    return nil
}
```

## PostgreSQL Implementation

### Configuration

`postgres.NewStore` accepts a `*postgres.StoreConfig` built with functional options:

```go
s := postgres.NewStore(postgres.NewStoreConfig(
    postgres.WithEventsTable("my_events"),           // default: "events"
    postgres.WithStreamHeadsTable("my_stream_heads"), // default: "stream_heads"
    postgres.WithLogger(myLogger),                   // optional; nil disables logging
))
```

`postgres.DefaultStoreConfig()` returns a ready-to-use configuration with default table names and no logger.

### NOTIFY Support

Configure the store to issue a `pg_notify` call inside each `Append` transaction. The notification fires only when the transaction commits — no phantom wakes.

```go
s := postgres.NewStore(postgres.NewStoreConfig(
    postgres.WithNotifyChannel("eventsalsa_events"),
))
```

Consumers can `LISTEN` on the same channel to wake up immediately instead of polling on a fixed interval.

## Migration Generator

`cmd/migrate-gen` generates a single `.sql` file that creates all required tables and indexes.

**Standard Tables (Default):**

```bash
go run github.com/eventsalsa/store/cmd/migrate-gen -output migrations
# writes migrations/20060102150405_init_event_sourcing.sql
```

**Native Range Partitioning:**

```bash
go run github.com/eventsalsa/store/cmd/migrate-gen \
  -output migrations \
  -partition-strategy native \
  -partition-size 10000000 \
  -initial-partitions 4
```

**`pg_partman` Managed Partitioning:**

```bash
go run github.com/eventsalsa/store/cmd/migrate-gen \
  -output migrations \
  -partition-strategy partman \
  -partman-maintenance pg_cron
```

For full DBA guides, maintenance automation, and background worker (`pg_partman_bgw`) setup, see [Range Partitioning Guide](docs/partitioning.md).

**`go:generate`:**

```go
//go:generate go run github.com/eventsalsa/store/cmd/migrate-gen -output migrations -filename init.sql
```

The generated migration creates:

- **`events`** — append-only event log with `global_position BIGSERIAL` (or sequence-backed in partitioned mode) primary key
- **`stream_heads`** — one row per stream tracking its current version for atomic stream version reservation and optimistic concurrency during `Append`

## Event Mapping Code Generator

`cmd/eventmap-gen` generates type-safe mapping code between your domain event structs and `store.Event` / `store.PersistedEvent`. This keeps your domain model free of infrastructure dependencies.

```bash
go run github.com/eventsalsa/store/cmd/eventmap-gen \
  -input internal/domain/events \
  -output internal/infrastructure/generated
```

See the [`eventmap-codegen`](./examples/eventmap-codegen/) example for a complete demonstration including versioned events and schema evolution patterns.

## Examples

Complete, runnable examples are in [`examples/`](./examples/):

- **[basic](./examples/basic/)** — connecting, appending events, reading streams, and reading the global log

- **[eventmap-codegen](./examples/eventmap-codegen/)** — generating type-safe domain event mappings with `eventmap-gen`, including versioned payloads and projections

## Development

**Unit tests:**

```bash
make test-unit
```

**Integration tests (requires Docker):**

```bash
make test-integration
```

Integration tests automatically launch and manage an isolated PostgreSQL container via [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go).

Or run via `go test` directly:

```bash
go test -p 1 -v -tags=integration ./...
```

**Lint and format:**

```bash
make lint
make fmt
```

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

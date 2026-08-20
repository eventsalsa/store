# Walkthrough: Range Partitioned Event Store & Optimistic Concurrency

We have implemented range partitioning on `global_position` for PostgreSQL event stores and refactored optimistic concurrency to be enforced authoritatively at the database level using `stream_heads`.

---

## Key Changes

### 1. Atomic Optimistic Concurrency with `stream_heads`
- **Location**: [`postgres/store.go`](postgres/store.go)
- **Mechanism**:
  - `Append()` atomically reserves stream versions on `stream_heads` in the caller's transaction before inserting into `events`.
  - **`NoStream()` / `Exact(0)`**: `INSERT INTO stream_heads (stream_type, stream_id, stream_version) VALUES ($1, $2, $count)` catches conflicts via primary key collision (`(stream_type, stream_id)`).
  - **`Exact(N)`**: `UPDATE stream_heads SET stream_version = stream_version + $count WHERE stream_version = $expected` catches conflicts via zero rows affected (`store.ErrOptimisticConcurrency`).
  - **`Any()`**: `INSERT INTO stream_heads ... ON CONFLICT DO UPDATE SET stream_version = stream_heads.stream_version + EXCLUDED.stream_version RETURNING stream_version` serializes concurrent unconditional appends into contiguous version sequences.

### 2. Range Partitioning Migration Generator
- **Location**: [`migrations/generator.go`](migrations/generator.go), [`cmd/migrate-gen/main.go`](cmd/migrate-gen/main.go)
- **Supported Modes**:
  - **Standard (Default)**: Single unpartitioned table (100% backward compatible).
  - **Native Range Partitioning (`-partition-strategy native`)**: Pre-allocated zero-padded partitions (e.g., `events_p0000000001_p0010000000`) with sequence-backed `global_position`.
  - **`pg_partman` Dynamic Partitioning (`-partition-strategy partman`)**:
    - `pg_cron` automated maintenance (`-partman-maintenance pg_cron`)
    - Background worker (`-partman-maintenance bgw`)
    - External / manual maintenance (`-partman-maintenance none`)

### 3. Comprehensive Gap & Stress Test Suites
- **Location**: [`postgres/integration_test/`](postgres/integration_test/)
  - `concurrency_test.go`: 8 high-concurrency race condition scenarios (50 goroutines per stream with `NoStream`, `Exact(0)`, `Exact(N)`, `Any()`, rollbacks, and contention).
  - `partitioning_test.go`: 6 partition lifecycle tests (cross-boundary appends, batch boundary straddling, multi-partition `ReadStream`, sequential pagination, `GetLatestGlobalPosition`, out-of-bounds partition errors).
  - `notify_test.go`: Real PostgreSQL `LISTEN`/`NOTIFY` integration testing and transaction rollback suppression.
  - `protocol_mode_test.go`: `QueryExecModeSimpleProtocol` (pgBouncer transaction pooling), 64KB binary payloads, null bytes, JSONB metadata round-trip, and input validation bounds.

### 4. Operator Documentation
- **Location**: [`docs/partitioning.md`](docs/partitioning.md), [`README.md`](README.md)
  - Architectural overview diagrams and rationale.
  - DBA guide for partition pre-creation and monitoring.

---

## Verification Results

### Unit Tests
```bash
go test ./...
# ok  github.com/eventsalsa/store
# ok  github.com/eventsalsa/store/consumer
# ok  github.com/eventsalsa/store/eventmap
# ok  github.com/eventsalsa/store/migrations
# ok  github.com/eventsalsa/store/postgres
```

### Full Integration Test Suite (PostgreSQL 16 via Testcontainers)
```bash
go test -count=1 -p 1 -v -tags=integration ./...
# 32 Integration test scenarios passed (0 failures)
```

### Linter
```bash
golangci-lint run
# 0 issues
```

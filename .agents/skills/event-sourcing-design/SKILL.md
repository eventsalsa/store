---
name: event-sourcing-design
description: Guidance on event store design, aggregate boundaries, optimistic concurrency, and CQRS patterns.
---

# Event Sourcing Architecture & Design

This skill provides guidelines and principles for designing and implementing event-sourced systems within this repository.

## Core Concepts

### 1. Event Store Design
*   **Append-Only Logs**: Event logs are strictly append-only. Once an event is written, it is immutable.
*   **Stream Partitioning**: Events are partitioned by aggregate type and aggregate ID.
*   **Ordering**: The store guarantees global ordering via a global position (`BIGSERIAL` in PostgreSQL) and per-stream ordering via aggregate versions.

### 2. Aggregates & Command Handling
*   **Consistency Boundaries**: Aggregates define the boundary for consistency. Always ensure command validation happens within aggregate boundaries.
*   **Invariant Enforcement**: All invariants must be validated against the current state of the aggregate before generating new events.

### 3. Optimistic Concurrency
*   **Version-Based Detection**: Conflict detection is based on the aggregate version.
*   **Expected Version Semantics**:
    *   `NoStream()`: Asserts that the stream must not already exist (for creation commands).
    *   `Exact(v)`: Asserts that the stream must be exactly at version `v`.
    *   `Any()`: Bypasses concurrency checks (use with caution).
*   **Conflict Recovery**: If `store.ErrOptimisticConcurrency` is returned, callers should retry the transaction.

### 4. Event Schema Evolution
*   **Immutability**: Never modify existing persisted events.
*   **Versioning**: When event structures change, use upcasting, event transformers, or design new event types to ensure backward compatibility.
*   **Avoid Breaking Changes**: Prefer adding optional fields to existing events rather than deleting or modifying existing fields.

### 5. Projections & Read Models
*   **Eventual Consistency**: Read models are projected from the event stream. Keep projection logic isolated from write-side aggregate internals.
*   **Pull-Based Consumers**: Consumers read the event stream sequentially using checkpoints to track progress.

## Anti-Patterns to Avoid

*   **Derived State**: Do not store derived or computed state within events. Only store the facts of the event.
*   **Message Bus Abuse**: Do not use the event store as a general-purpose message bus between bounded contexts without clear contracts.
*   **Leaking Internals**: Do not couple projections directly to the internal state representations of aggregates.

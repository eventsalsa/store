package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrOptimisticConcurrency indicates a version conflict during append.
	ErrOptimisticConcurrency = errors.New("optimistic concurrency conflict")

	// ErrNoEvents indicates an attempt to append zero events.
	ErrNoEvents = errors.New("no events to append")
)

// EventStore defines the interface for appending events.
type EventStore interface {
	// Append atomically appends one or more events within the provided transaction.
	// Events must all belong to the same stream instance.
	// Returns an AppendResult containing the persisted events with assigned versions
	// and their global positions, or an error.
	//
	// The expectedVersion parameter controls optimistic concurrency:
	// - Any(): No version check - always succeeds if no other errors
	// - NoStream(): Stream must not exist - used for stream creation
	// - Exact(N): Stream must be at version N - used for normal updates
	//
	// The store automatically assigns StreamVersion to each event:
	// - Fetches the current version from the stream_heads table (O(1) lookup)
	// - Validates against expectedVersion
	// - Assigns consecutive versions starting from (current + 1)
	// - Updates stream_heads with the new version
	// - The database unique constraint on (stream_type, stream_id, stream_version)
	//   enforces optimistic concurrency as a last safety net
	//
	// Returns ErrOptimisticConcurrency if expectedVersion validation fails or if
	// another transaction commits conflicting events between the version check and insert
	// (detected via unique constraint violation).
	// Returns ErrNoEvents if events slice is empty.
	//
	// After a successful append:
	// - Use result.ToVersion() to get the new stream version
	// - Use result.Events to access the persisted events with all fields populated
	// - Use result.GlobalPositions to get the assigned global positions
	Append(ctx context.Context, tx pgx.Tx, expectedVersion ExpectedVersion, events []Event) (AppendResult, error)
}

// EventReader defines the interface for reading events sequentially.
type EventReader interface {
	// ReadEvents reads events starting from the given global position.
	// Returns up to limit events ordered by global_position ascending.
	//
	// WARNING: global_position is BIGSERIAL-backed. PostgreSQL sequences guarantee
	// uniqueness, not commit order. A lower position allocated by a concurrent
	// transaction may become visible after a higher one has already been returned.
	// Advancing a checkpoint to the highest seen position without accounting for
	// in-flight gaps can permanently skip events. Async consumers must use a
	// gap-aware runtime; do not treat the highest returned position as a safe
	// checkpoint frontier under concurrent writers.
	ReadEvents(ctx context.Context, tx pgx.Tx, fromPosition int64, limit int) ([]PersistedEvent, error)
}

// GlobalPositionReader defines the interface for reading the latest global event position.
// This is useful for lightweight "new events available" checks without loading full batches.
type GlobalPositionReader interface {
	// GetLatestGlobalPosition returns the highest global_position currently present in the event log.
	// Returns 0 when no events exist.
	//
	// WARNING: Because global_position is BIGSERIAL-backed, the returned value is not a safe
	// checkpoint frontier under concurrent writers. A concurrent transaction holding a lower
	// position may commit after this call returns, making that position invisible to any
	// consumer that has already advanced its checkpoint past it.
	GetLatestGlobalPosition(ctx context.Context, tx pgx.Tx) (int64, error)
}

// StreamReader defines the interface for reading events for a specific stream.
type StreamReader interface {
	// ReadStream reads all events for a specific stream instance and returns
	// them as a Stream containing the stream's full history.
	// Events are ordered by stream_version ascending.
	//
	// Parameters:
	// - streamType: the type of stream (e.g., "User", "Order")
	// - streamID: the unique identifier of the stream instance (can be UUID string, email, etc.)
	// - fromVersion: optional minimum version (inclusive). Pass nil to read from the beginning.
	// - toVersion: optional maximum version (inclusive). Pass nil to read to the end.
	//
	// Examples:
	// - ReadStream(ctx, tx, "User", "550e8400-e29b-41d4-a716-446655440000", nil, nil) - read all events
	// - ReadStream(ctx, tx, "User", id, ptr(5), nil) - read from version 5 onwards
	// - ReadStream(ctx, tx, "User", id, nil, ptr(10)) - read up to version 10
	// - ReadStream(ctx, tx, "User", id, ptr(5), ptr(10)) - read versions 5-10
	//
	// Returns a Stream with an empty Events slice if no events match the criteria.
	// Use stream.Version() to get the current stream version.
	// Use stream.IsEmpty() to check if any events were found.
	ReadStream(ctx context.Context, tx pgx.Tx, streamType string, streamID string, fromVersion, toVersion *int64) (Stream, error)
}

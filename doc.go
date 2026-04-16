// Package store provides core event sourcing types and persistence interfaces.
//
// This package defines the fundamental building blocks for event sourcing:
//
//   - Event types: Event (before persistence) and PersistedEvent (after persistence)
//   - Stream: Full history for a single aggregate
//   - Store interfaces: EventStore, EventReader, AggregateStreamReader
//   - Optimistic concurrency: ExpectedVersion with Any, NoStream, and Exact modes
//   - Observability: Logger interface for optional structured logging
//
// The postgres package provides the PostgreSQL implementation of these interfaces.
//
// Example usage:
//
//	store := postgres.NewStore(postgres.DefaultStoreConfig())
//	tx, _ := db.BeginTx(ctx, nil)
//	result, err := store.Append(ctx, tx, store.NoStream(), events)
//	tx.Commit()
package store

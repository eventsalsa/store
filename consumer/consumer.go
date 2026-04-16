// Package consumer provides event consumer interface definitions.
//
// Consumers are the units of event processing in an event-sourced system.
// They receive persisted events and can update read models, send notifications,
// publish to message brokers, or perform any side effect.
//
// This package defines only the consumer contracts. Processing infrastructure
// (workers, runners, segment processors) lives in separate packages. That
// infrastructure is responsible for gap-aware checkpoint advancement; see
// store.EventReader for the global-position gap caveat under concurrent writers.
package consumer

import (
	"context"
	"database/sql"

	"github.com/eventsalsa/store"
)

// Consumer defines the interface for event consumer handlers.
// Consumers are storage-agnostic and can write to any destination
// (SQL databases, NoSQL stores, message brokers, search engines, etc.).
type Consumer interface {
	// Name returns the unique name of this consumer.
	// This name is used for checkpoint tracking.
	Name() string

	// Handle processes a single event.
	// Return an error to stop consumer processing.
	//
	// The tx parameter is the processor's transaction used for checkpoint management.
	// SQL consumers can use this transaction to ensure atomic updates of both
	// the read model and the checkpoint. This eliminates inconsistencies where
	// a consumer succeeds but the checkpoint update fails (or vice versa).
	// It does not by itself guarantee that checkpointing to the highest seen
	// global position is safe; processors still need a gap-aware runtime/frontier
	// before advancing checkpoints under concurrent writers.
	//
	// The transaction will be committed by the processor after Handle returns successfully.
	// Consumers should NEVER call Commit() or Rollback() on the provided transaction.
	//
	// For non-SQL consumers (Elasticsearch, Redis, message brokers), the tx parameter
	// should be ignored and consumers should manage their own connections as before.
	//
	// Event is passed by value to enforce immutability (events are value objects).
	// Large data (Payload, Metadata byte slices) share references to their backing arrays,
	// so the actual payload/metadata data is not deep-copied.
	//
	//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
	Handle(ctx context.Context, tx *sql.Tx, event store.PersistedEvent) error
}

// ScopedConsumer is an optional interface that consumers can implement to filter
// events by aggregate type. This is useful for read model consumers that only
// care about specific aggregate types.
//
// By default, consumers implementing only the Consumer interface receive all events.
// This ensures that global consumers (e.g., integration publishers, audit logs) continue
// to work without modification.
//
// Example - Read model consumer scoped to User aggregate:
//
//	type UserReadModelConsumer struct {}
//
//	func (p *UserReadModelConsumer) Name() string {
//	   return "user_read_model"
//	}
//
//	func (p *UserReadModelConsumer) AggregateTypes() []string {
//	   return []string{"User"}
//	}
//
//	func (p *UserReadModelConsumer) Handle(ctx context.Context, tx *sql.Tx, event store.PersistedEvent) error {
//	   // Only receives User aggregate events
//	   // Use tx for atomic read model updates with checkpoint
//	   return nil
//	}
//
// Example - Global integration publisher:
//
//	type WatermillPublisher struct {}
//
//	func (p *WatermillPublisher) Name() string {
//	   return "system.integration.watermill.v1"
//	}
//
//	func (p *WatermillPublisher) Handle(ctx context.Context, tx *sql.Tx, event store.PersistedEvent) error {
//	   // Receives ALL events for publishing to message broker
//	   // Ignore tx parameter - use message broker client
//	   _ = tx
//	   return nil
//	}
type ScopedConsumer interface {
	Consumer
	// AggregateTypes returns the list of aggregate types this consumer cares about.
	// If empty, the consumer receives events from all aggregate types.
	// If non-empty, only events matching one of these aggregate types are passed to Handle.
	AggregateTypes() []string
}

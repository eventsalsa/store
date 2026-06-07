// Package postgres provides a PostgreSQL implementation for the event store.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/eventsalsa/store"
)

// StoreConfig contains configuration for the PostgreSQL event store.
// Configuration is immutable after construction.
type StoreConfig struct {
	// Logger is an optional logger for observability.
	// If nil, logging is disabled (zero overhead).
	Logger store.Logger

	// EventsTable is the name of the events table
	EventsTable string

	// AggregateHeadsTable is the name of the aggregate version tracking table
	AggregateHeadsTable string

	// NotifyChannel is the Postgres NOTIFY channel name for event append notifications.
	// When set, Append() executes pg_notify within the same transaction, so the
	// notification fires only when the transaction commits.
	// Leave empty to disable notifications.
	NotifyChannel string
}

// DefaultStoreConfig returns the default configuration.
func DefaultStoreConfig() *StoreConfig {
	return &StoreConfig{
		EventsTable:         "events",
		AggregateHeadsTable: "aggregate_heads",
		Logger:              nil, // No logging by default
	}
}

// StoreOption is a functional option for configuring a Store.
type StoreOption func(*StoreConfig)

// WithLogger sets a logger for the store.
func WithLogger(logger store.Logger) StoreOption {
	return func(c *StoreConfig) {
		c.Logger = logger
	}
}

// WithEventsTable sets a custom events table name.
func WithEventsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.EventsTable = tableName
	}
}

// WithAggregateHeadsTable sets a custom aggregate heads table name.
func WithAggregateHeadsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.AggregateHeadsTable = tableName
	}
}

// WithNotifyChannel sets the Postgres NOTIFY channel for event append notifications.
// When configured, each Append() call issues pg_notify within the same transaction,
// so the notification fires only when the transaction commits.
func WithNotifyChannel(channel string) StoreOption {
	return func(c *StoreConfig) {
		c.NotifyChannel = channel
	}
}

// NewStoreConfig creates a new store configuration with functional options.
// It starts with the default configuration and applies the given options.
//
// Example:
//
//	config := postgres.NewStoreConfig(
//	    postgres.WithLogger(myLogger),
//	    postgres.WithEventsTable("custom_events"),
//	)
func NewStoreConfig(opts ...StoreOption) *StoreConfig {
	config := DefaultStoreConfig()
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// Store is a PostgreSQL-backed event store implementation.
type Store struct {
	config StoreConfig
}

// NewStore creates a new PostgreSQL event store with the given configuration.
func NewStore(config *StoreConfig) *Store {
	return &Store{
		config: *config,
	}
}

// Append implements store.EventStore.
// It automatically assigns aggregate versions using the aggregate_heads table for O(1) lookup.
// The expectedVersion parameter controls optimistic concurrency validation.
// The database constraint on (aggregate_type, aggregate_id, aggregate_version) enforces
// optimistic concurrency as a safety net - if another transaction commits between our version
// check and insert, the insert will fail with a unique constraint violation.
//
//nolint:gocyclo // Cyclomatic complexity is acceptable here - comes from necessary logging and validation checks
func (s *Store) Append(ctx context.Context, tx pgx.Tx, expectedVersion store.ExpectedVersion, events []store.Event) (store.AppendResult, error) {
	if len(events) == 0 {
		return store.AppendResult{}, store.ErrNoEvents
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "append starting",
			"event_count", len(events),
			"expected_version", expectedVersion.String())
	}

	// Validate all events belong to same aggregate
	firstEvent := events[0]
	for i := range events {
		e := &events[i]
		if e.AggregateType != firstEvent.AggregateType {
			return store.AppendResult{}, fmt.Errorf("event %d: aggregate type mismatch", i)
		}
		if e.AggregateID != firstEvent.AggregateID {
			return store.AppendResult{}, fmt.Errorf("event %d: aggregate ID mismatch", i)
		}
	}

	// Fetch current version from aggregate_heads table
	var currentVersion *int64
	//nolint:gosec // G201: table name from trusted config, not user input
	query := fmt.Sprintf(`
		SELECT aggregate_version 
		FROM %s 
		WHERE aggregate_type = $1 AND aggregate_id = $2
	`, s.config.AggregateHeadsTable)

	err := tx.QueryRow(ctx, query, firstEvent.AggregateType, firstEvent.AggregateID).Scan(&currentVersion)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return store.AppendResult{}, fmt.Errorf("failed to check current version: %w", err)
	}

	// Validate expected version
	if !expectedVersion.IsAny() {
		if expectedVersion.IsNoStream() {
			if currentVersion != nil {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "expected version validation failed: aggregate already exists",
						"aggregate_type", firstEvent.AggregateType,
						"aggregate_id", firstEvent.AggregateID,
						"current_version", *currentVersion,
						"expected_version", expectedVersion.String())
				}
				return store.AppendResult{}, store.ErrOptimisticConcurrency
			}
		} else if expectedVersion.IsExact() {
			if currentVersion == nil {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "expected version validation failed: aggregate does not exist",
						"aggregate_type", firstEvent.AggregateType,
						"aggregate_id", firstEvent.AggregateID,
						"expected_version", expectedVersion.String())
				}
				return store.AppendResult{}, store.ErrOptimisticConcurrency
			}
			if *currentVersion != expectedVersion.Value() {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "expected version validation failed: version mismatch",
						"aggregate_type", firstEvent.AggregateType,
						"aggregate_id", firstEvent.AggregateID,
						"current_version", *currentVersion,
						"expected_version", expectedVersion.String())
				}
				return store.AppendResult{}, store.ErrOptimisticConcurrency
			}
		}
	}

	// Determine starting version for new events
	var nextVersion int64
	if currentVersion != nil {
		nextVersion = *currentVersion + 1
	} else {
		nextVersion = 1
	}

	if s.config.Logger != nil {
		if currentVersion != nil {
			s.config.Logger.Debug(ctx, "version calculated",
				"aggregate_type", firstEvent.AggregateType,
				"aggregate_id", firstEvent.AggregateID,
				"current_version", *currentVersion,
				"next_version", nextVersion)
		} else {
			s.config.Logger.Debug(ctx, "version calculated",
				"aggregate_type", firstEvent.AggregateType,
				"aggregate_id", firstEvent.AggregateID,
				"current_version", "none",
				"next_version", nextVersion)
		}
	}

	// Insert events with auto-assigned versions and collect global positions and persisted events
	globalPositions := make([]int64, len(events))
	persistedEvents := make([]store.PersistedEvent, len(events))
	//nolint:gosec // G201: table name from trusted config, not user input
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			aggregate_type, aggregate_id, aggregate_version,
			event_id, event_type, event_version,
			payload, trace_id, correlation_id, causation_id,
			metadata, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING global_position
	`, s.config.EventsTable)

	for i := range events {
		event := &events[i]
		aggregateVersion := nextVersion + int64(i)

		// Convert metadata []byte to string or nil to ensure compatibility with
		// pgx's simple protocol mode (useful for pg_bouncer transaction mode).
		var metadata any
		if len(event.Metadata) > 0 {
			metadata = string(event.Metadata)
		}

		var globalPos int64
		err = tx.QueryRow(ctx, insertQuery,
			event.AggregateType,
			event.AggregateID,
			aggregateVersion,
			event.EventID,
			event.EventType,
			event.EventVersion,
			event.Payload,
			event.TraceID,
			event.CorrelationID,
			event.CausationID,
			metadata,
			event.CreatedAt,
		).Scan(&globalPos)

		if err != nil {
			if IsUniqueViolation(err) {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "optimistic concurrency conflict",
						"aggregate_type", event.AggregateType,
						"aggregate_id", event.AggregateID,
						"aggregate_version", aggregateVersion)
				}
				return store.AppendResult{}, store.ErrOptimisticConcurrency
			}
			return store.AppendResult{}, fmt.Errorf("failed to insert event %d: %w", i, err)
		}
		globalPositions[i] = globalPos

		persistedEvents[i] = store.PersistedEvent{
			GlobalPosition:   globalPos,
			AggregateType:    event.AggregateType,
			AggregateID:      event.AggregateID,
			AggregateVersion: aggregateVersion,
			EventID:          event.EventID,
			EventType:        event.EventType,
			EventVersion:     event.EventVersion,
			Payload:          event.Payload,
			TraceID:          event.TraceID,
			CorrelationID:    event.CorrelationID,
			CausationID:      event.CausationID,
			Metadata:         event.Metadata,
			CreatedAt:        event.CreatedAt,
		}
	}

	// Update aggregate_heads with the new version (UPSERT pattern)
	latestVersion := nextVersion + int64(len(events)) - 1
	//nolint:gosec // G201: table name from trusted config, not user input
	upsertQuery := fmt.Sprintf(`
		INSERT INTO %s (aggregate_type, aggregate_id, aggregate_version, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (aggregate_type, aggregate_id)
		DO UPDATE SET aggregate_version = $3, updated_at = NOW()
	`, s.config.AggregateHeadsTable)

	_, err = tx.Exec(ctx, upsertQuery, firstEvent.AggregateType, firstEvent.AggregateID, latestVersion)
	if err != nil {
		return store.AppendResult{}, fmt.Errorf("failed to update aggregate head: %w", err)
	}

	// Send transactional NOTIFY — fires only when the caller commits the TX
	if s.config.NotifyChannel != "" {
		lastPos := globalPositions[len(globalPositions)-1]
		_, err = tx.Exec(ctx, "SELECT pg_notify($1, $2)", s.config.NotifyChannel, fmt.Sprintf("%d", lastPos))
		if err != nil {
			return store.AppendResult{}, fmt.Errorf("failed to send notify: %w", err)
		}
	}

	if s.config.Logger != nil {
		s.config.Logger.Info(ctx, "events appended",
			"aggregate_type", firstEvent.AggregateType,
			"aggregate_id", firstEvent.AggregateID,
			"event_count", len(events),
			"version_range", fmt.Sprintf("%d-%d", nextVersion, latestVersion),
			"positions", globalPositions)
	}

	return store.AppendResult{
		Events:          persistedEvents,
		GlobalPositions: globalPositions,
	}, nil
}

const uniqueViolationSQLState = "23505"

// IsUniqueViolation checks if an error is a PostgreSQL unique constraint violation.
// This is exported for testing purposes.
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == uniqueViolationSQLState
	}
	return false
}

// ReadEvents implements store.EventReader.
func (s *Store) ReadEvents(ctx context.Context, tx pgx.Tx, fromPosition int64, limit int) ([]store.PersistedEvent, error) {
	return s.readEvents(ctx, tx, fromPosition, limit)
}

func (s *Store) readEvents(ctx context.Context, tx pgx.Tx, fromPosition int64, limit int) ([]store.PersistedEvent, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "reading events", "from_position", fromPosition, "limit", limit)
	}

	query, args := buildReadEventsQuery(s.config.EventsTable, fromPosition, limit)
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []store.PersistedEvent
	for rows.Next() {
		var e store.PersistedEvent
		err := rows.Scan(
			&e.GlobalPosition,
			&e.AggregateType,
			&e.AggregateID,
			&e.AggregateVersion,
			&e.EventID,
			&e.EventType,
			&e.EventVersion,
			&e.Payload,
			&e.TraceID,
			&e.CorrelationID,
			&e.CausationID,
			&e.Metadata,
			&e.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "events read", "count", len(events))
	}

	return events, nil
}

func buildReadEventsQuery(
	eventsTable string,
	fromPosition int64,
	limit int,
) (query string, args []any) {
	query = fmt.Sprintf(`
		SELECT 
			global_position, aggregate_type, aggregate_id, aggregate_version,
			event_id, event_type, event_version,
			payload, trace_id, correlation_id, causation_id,
			metadata, created_at
		FROM %s
		WHERE global_position > $1
	`, eventsTable)

	query += "\n\t\tORDER BY global_position ASC\n\t\tLIMIT $2\n\t"
	args = []any{fromPosition, limit}

	return query, args
}

// GetLatestGlobalPosition implements store.GlobalPositionReader.
func (s *Store) GetLatestGlobalPosition(ctx context.Context, tx pgx.Tx) (int64, error) {
	//nolint:gosec // G201: table name from trusted config, not user input
	query := fmt.Sprintf(`
		SELECT global_position
		FROM %s
		ORDER BY global_position DESC
		LIMIT 1
	`, s.config.EventsTable)

	var position int64
	err := tx.QueryRow(ctx, query).Scan(&position)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return position, nil
}

// ReadAggregateStream implements store.AggregateStreamReader.
func (s *Store) ReadAggregateStream(ctx context.Context, tx pgx.Tx, aggregateType, aggregateID string, fromVersion, toVersion *int64) (store.Stream, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "reading aggregate stream",
			"aggregate_type", aggregateType,
			"aggregate_id", aggregateID,
			"from_version", fromVersion,
			"to_version", toVersion)
	}

	//nolint:gosec // G201: table name from trusted config, not user input
	baseQuery := fmt.Sprintf(`
		SELECT 
			global_position, aggregate_type, aggregate_id, aggregate_version,
			event_id, event_type, event_version,
			payload, trace_id, correlation_id, causation_id,
			metadata, created_at
		FROM %s
		WHERE aggregate_type = $1 AND aggregate_id = $2
	`, s.config.EventsTable)

	var args []any
	args = append(args, aggregateType, aggregateID)
	paramIndex := 3

	if fromVersion != nil {
		baseQuery += fmt.Sprintf(" AND aggregate_version >= $%d", paramIndex)
		args = append(args, *fromVersion)
		paramIndex++
	}

	if toVersion != nil {
		baseQuery += fmt.Sprintf(" AND aggregate_version <= $%d", paramIndex)
		args = append(args, *toVersion)
	}

	baseQuery += " ORDER BY aggregate_version ASC"

	rows, err := tx.Query(ctx, baseQuery, args...)
	if err != nil {
		return store.Stream{}, fmt.Errorf("failed to query aggregate stream: %w", err)
	}
	defer rows.Close()

	var events []store.PersistedEvent
	for rows.Next() {
		var e store.PersistedEvent
		err := rows.Scan(
			&e.GlobalPosition,
			&e.AggregateType,
			&e.AggregateID,
			&e.AggregateVersion,
			&e.EventID,
			&e.EventType,
			&e.EventVersion,
			&e.Payload,
			&e.TraceID,
			&e.CorrelationID,
			&e.CausationID,
			&e.Metadata,
			&e.CreatedAt,
		)
		if err != nil {
			return store.Stream{}, fmt.Errorf("failed to scan event: %w", err)
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return store.Stream{}, fmt.Errorf("rows error: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "aggregate stream read",
			"aggregate_type", aggregateType,
			"aggregate_id", aggregateID,
			"event_count", len(events))
	}

	return store.Stream{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Events:        events,
	}, nil
}

// Package postgres provides a PostgreSQL implementation for the event store.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
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

	// StreamHeadsTable is the name of the stream version tracking table
	StreamHeadsTable string

	// EventIDsTable is the optional companion table for enforcing global event_id uniqueness across partitions.
	// If empty, companion table writes are skipped.
	EventIDsTable string

	// NotifyChannel is the Postgres NOTIFY channel name for event append notifications.
	// When set, Append() executes pg_notify within the same transaction, so the
	// notification fires only when the transaction commits.
	// Leave empty to disable notifications.
	NotifyChannel string
}

// DefaultStoreConfig returns the default configuration.
func DefaultStoreConfig() *StoreConfig {
	return &StoreConfig{
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
		EventIDsTable:    "",
		Logger:           nil, // No logging by default
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

// WithStreamHeadsTable sets a custom stream heads table name.
func WithStreamHeadsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.StreamHeadsTable = tableName
	}
}

// WithEventIDsTable sets the companion event IDs table name for global uniqueness in partitioned layouts.
func WithEventIDsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.EventIDsTable = tableName
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
// It atomically reserves stream versions using the stream_heads table before inserting events.
// The expectedVersion parameter controls optimistic concurrency validation directly in SQL:
// - NoStream() / Exact(0): attempts to insert the initial stream head row; fails if already present
// - Exact(N): conditionally updates the stream head matching expected version; fails if mismatch
// - Any(): atomically increments or creates the stream head returning the reserved version range
//
// Returns store.ErrOptimisticConcurrency if expectedVersion validation fails.
// Returns store.ErrNoEvents if events slice is empty.
func (s *Store) Append(ctx context.Context, tx pgx.Tx, expectedVersion store.ExpectedVersion, events []store.Event) (store.AppendResult, error) {
	if len(events) == 0 {
		return store.AppendResult{}, store.ErrNoEvents
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "append starting",
			"event_count", len(events),
			"expected_version", expectedVersion.String())
	}

	if err := validateHomogeneousStream(events); err != nil {
		return store.AppendResult{}, err
	}

	firstEvent := events[0]

	// Reserve stream version range atomically on stream_heads table
	vRange, err := s.reserveStreamVersion(ctx, tx, firstEvent.StreamType, firstEvent.StreamID, expectedVersion, len(events))
	if err != nil {
		return store.AppendResult{}, err
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "version range reserved",
			"stream_type", firstEvent.StreamType,
			"stream_id", firstEvent.StreamID,
			"first_version", vRange.firstVersion,
			"final_version", vRange.finalVersion)
	}

	// Insert events with auto-assigned versions and collect global positions and persisted events
	globalPositions := make([]int64, len(events))
	persistedEvents := make([]store.PersistedEvent, len(events))
	//nolint:gosec // G201: table name from trusted config, not user input
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			stream_type, stream_id, stream_version,
			event_id, event_type, event_version,
			payload, trace_id, correlation_id, causation_id,
			metadata, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING global_position
	`, s.config.EventsTable)

	for i := range events {
		event := &events[i]
		streamVersion := vRange.firstVersion + int64(i)

		// Convert metadata []byte to string or nil to ensure compatibility with
		// pgx's simple protocol mode (useful for pg_bouncer transaction mode).
		var metadata any
		if len(event.Metadata) > 0 {
			metadata = string(event.Metadata)
		}

		var globalPos int64
		err = tx.QueryRow(ctx, insertQuery,
			event.StreamType,
			event.StreamID,
			streamVersion,
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
						"stream_type", event.StreamType,
						"stream_id", event.StreamID,
						"stream_version", streamVersion)
				}
				return store.AppendResult{}, store.ErrOptimisticConcurrency
			}
			return store.AppendResult{}, fmt.Errorf("failed to insert event %d: %w", i, err)
		}
		globalPositions[i] = globalPos

		if err := s.recordEventID(ctx, tx, event.EventID, globalPos, event.CreatedAt); err != nil {
			return store.AppendResult{}, err
		}

		persistedEvents[i] = store.PersistedEvent{
			GlobalPosition: globalPos,
			StreamType:     event.StreamType,
			StreamID:       event.StreamID,
			StreamVersion:  streamVersion,
			EventID:        event.EventID,
			EventType:      event.EventType,
			EventVersion:   event.EventVersion,
			Payload:        event.Payload,
			TraceID:        event.TraceID,
			CorrelationID:  event.CorrelationID,
			CausationID:    event.CausationID,
			Metadata:       event.Metadata,
			CreatedAt:      event.CreatedAt,
		}
	}

	// Send transactional NOTIFY — fires only when the caller commits the TX
	if err := s.sendNotification(ctx, tx, globalPositions[len(globalPositions)-1]); err != nil {
		return store.AppendResult{}, err
	}

	if s.config.Logger != nil {
		s.config.Logger.Info(ctx, "events appended",
			"stream_type", firstEvent.StreamType,
			"stream_id", firstEvent.StreamID,
			"event_count", len(events),
			"version_range", fmt.Sprintf("%d-%d", vRange.firstVersion, vRange.finalVersion),
			"positions", globalPositions)
	}

	return store.AppendResult{
		Events:          persistedEvents,
		GlobalPositions: globalPositions,
	}, nil
}

type versionRange struct {
	firstVersion int64
	finalVersion int64
}

func (s *Store) reserveStreamVersion(
	ctx context.Context,
	tx pgx.Tx,
	streamType, streamID string,
	expectedVersion store.ExpectedVersion,
	count int,
) (versionRange, error) {
	numEvents := int64(count)

	if expectedVersion.IsNoStream() || (expectedVersion.IsExact() && expectedVersion.Value() == 0) {
		//nolint:gosec // G201: table name from trusted config, not user input
		query := fmt.Sprintf(`
			INSERT INTO %s (stream_type, stream_id, stream_version, updated_at)
			VALUES ($1, $2, $3, NOW())
		`, s.config.StreamHeadsTable)

		_, err := tx.Exec(ctx, query, streamType, streamID, numEvents)
		if err != nil {
			if IsUniqueViolation(err) {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "expected version validation failed: stream already exists",
						"stream_type", streamType,
						"stream_id", streamID,
						"expected_version", expectedVersion.String())
				}
				return versionRange{}, store.ErrOptimisticConcurrency
			}
			return versionRange{}, fmt.Errorf("failed to insert stream head: %w", err)
		}

		return versionRange{
			firstVersion: 1,
			finalVersion: numEvents,
		}, nil
	}

	if expectedVersion.IsExact() {
		expected := expectedVersion.Value()
		//nolint:gosec // G201: table name from trusted config, not user input
		query := fmt.Sprintf(`
			UPDATE %s
			SET stream_version = stream_version + $3, updated_at = NOW()
			WHERE stream_type = $1 AND stream_id = $2 AND stream_version = $4
		`, s.config.StreamHeadsTable)

		tag, err := tx.Exec(ctx, query, streamType, streamID, numEvents, expected)
		if err != nil {
			return versionRange{}, fmt.Errorf("failed to update stream head: %w", err)
		}

		if tag.RowsAffected() == 0 {
			if s.config.Logger != nil {
				s.config.Logger.Error(ctx, "expected version validation failed: version mismatch or stream does not exist",
					"stream_type", streamType,
					"stream_id", streamID,
					"expected_version", expectedVersion.String())
			}
			return versionRange{}, store.ErrOptimisticConcurrency
		}

		return versionRange{
			firstVersion: expected + 1,
			finalVersion: expected + numEvents,
		}, nil
	}

	// Any() expected version - atomic UPSERT with increment
	//nolint:gosec // G201: table name from trusted config, not user input
	query := fmt.Sprintf(`
		INSERT INTO %s (stream_type, stream_id, stream_version, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (stream_type, stream_id)
		DO UPDATE SET 
			stream_version = %s.stream_version + EXCLUDED.stream_version,
			updated_at = NOW()
		RETURNING stream_version
	`, s.config.StreamHeadsTable, s.config.StreamHeadsTable)

	var finalVersion int64
	err := tx.QueryRow(ctx, query, streamType, streamID, numEvents).Scan(&finalVersion)
	if err != nil {
		return versionRange{}, fmt.Errorf("failed to reserve stream version range: %w", err)
	}

	return versionRange{
		firstVersion: finalVersion - numEvents + 1,
		finalVersion: finalVersion,
	}, nil
}

func validateHomogeneousStream(events []store.Event) error {
	first := events[0]
	for i := range events {
		e := &events[i]
		if e.StreamType != first.StreamType {
			return fmt.Errorf("event %d: stream type mismatch", i)
		}
		if e.StreamID != first.StreamID {
			return fmt.Errorf("event %d: stream ID mismatch", i)
		}
	}
	return nil
}

func (s *Store) recordEventID(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, globalPos int64, createdAt time.Time) error {
	if s.config.EventIDsTable == "" {
		return nil
	}
	//nolint:gosec // G201: table name from trusted config, not user input
	dedupQuery := fmt.Sprintf(`
		INSERT INTO %s (event_id, global_position, created_at)
		VALUES ($1, $2, $3)
	`, s.config.EventIDsTable)

	_, err := tx.Exec(ctx, dedupQuery, eventID, globalPos, createdAt)
	if err != nil {
		if IsUniqueViolation(err) {
			if s.config.Logger != nil {
				s.config.Logger.Error(ctx, "duplicate event_id detected",
					"event_id", eventID.String(),
					"global_position", globalPos)
			}
			return fmt.Errorf("duplicate event ID %s: %w", eventID, err)
		}
		return fmt.Errorf("failed to record event id: %w", err)
	}
	return nil
}

func (s *Store) sendNotification(ctx context.Context, tx pgx.Tx, lastPos int64) error {
	if s.config.NotifyChannel == "" {
		return nil
	}
	_, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", s.config.NotifyChannel, fmt.Sprintf("%d", lastPos))
	if err != nil {
		return fmt.Errorf("failed to send notify: %w", err)
	}
	return nil
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
			&e.StreamType,
			&e.StreamID,
			&e.StreamVersion,
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
			global_position, stream_type, stream_id, stream_version,
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

// ReadStream implements store.StreamReader.
func (s *Store) ReadStream(ctx context.Context, tx pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "reading stream",
			"stream_type", streamType,
			"stream_id", streamID,
			"from_version", fromVersion,
			"to_version", toVersion)
	}

	//nolint:gosec // G201: table name from trusted config, not user input
	baseQuery := fmt.Sprintf(`
		SELECT 
			global_position, stream_type, stream_id, stream_version,
			event_id, event_type, event_version,
			payload, trace_id, correlation_id, causation_id,
			metadata, created_at
		FROM %s
		WHERE stream_type = $1 AND stream_id = $2
	`, s.config.EventsTable)

	var args []any
	args = append(args, streamType, streamID)
	paramIndex := 3

	if fromVersion != nil {
		baseQuery += fmt.Sprintf(" AND stream_version >= $%d", paramIndex)
		args = append(args, *fromVersion)
		paramIndex++
	}

	if toVersion != nil {
		baseQuery += fmt.Sprintf(" AND stream_version <= $%d", paramIndex)
		args = append(args, *toVersion)
	}

	baseQuery += " ORDER BY stream_version ASC"

	rows, err := tx.Query(ctx, baseQuery, args...)
	if err != nil {
		return store.Stream{}, fmt.Errorf("failed to query stream: %w", err)
	}
	defer rows.Close()

	var events []store.PersistedEvent
	for rows.Next() {
		var e store.PersistedEvent
		err := rows.Scan(
			&e.GlobalPosition,
			&e.StreamType,
			&e.StreamID,
			&e.StreamVersion,
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
		s.config.Logger.Debug(ctx, "stream read",
			"stream_type", streamType,
			"stream_id", streamID,
			"event_count", len(events))
	}

	return store.Stream{
		StreamType: streamType,
		StreamID:   streamID,
		Events:     events,
	}, nil
}

// Package integration_test contains integration tests for the Postgres adapter.
// These tests require a running PostgreSQL instance.
//
// Run with: go test -tags=integration ./postgres/integration_test/...
//
//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/eventsalsa/store"
	"github.com/eventsalsa/store/migrations"
	"github.com/eventsalsa/store/postgres"
)

type txOptions struct {
	ReadOnly bool
}

type testDB struct {
	*pgxpool.Pool
}

func (db testDB) BeginTx(ctx context.Context, opts *txOptions) (pgx.Tx, error) {
	if opts != nil && opts.ReadOnly {
		return db.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	}
	return db.Pool.Begin(ctx)
}

func (db testDB) PingContext(ctx context.Context) error {
	return db.Ping(ctx)
}

func (db testDB) QueryRowContext(ctx context.Context, query string, args ...any) pgx.Row {
	return db.QueryRow(ctx, query, args...)
}

func (db testDB) QueryContext(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	return db.Query(ctx, query, args...)
}

func getTestDB(t *testing.T) testDB {
	t.Helper()

	// Default to localhost, but allow override via env var for CI
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "postgres"
	}

	dbname := os.Getenv("POSTGRES_DB")
	if dbname == "" {
		dbname = "eventsalsa_test"
	}

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse connection string: %v", err)
	}

	if os.Getenv("PGX_TEST_SIMPLE_PROTOCOL") == "true" {
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("Failed to ping database: %v", err)
	}

	return testDB{pool}
}

func setupTestTables(t *testing.T, db testDB) {
	t.Helper()
	ctx := context.Background()

	// Drop existing objects to ensure clean state
	_, err := db.Exec(ctx, `
		DROP TABLE IF EXISTS aggregate_heads CASCADE;
		DROP TABLE IF EXISTS events CASCADE;
	`)
	if err != nil {
		t.Fatalf("Failed to drop tables: %v", err)
	}

	// Generate and execute migration
	tmpDir := t.TempDir()
	config := &migrations.Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "test.sql",
		EventsTable:         "events",
		AggregateHeadsTable: "aggregate_heads",
	}

	if err := migrations.GeneratePostgres(config); err != nil {
		t.Fatalf("Failed to generate migration: %v", err)
	}

	migrationSQL, err := os.ReadFile(fmt.Sprintf("%s/%s", tmpDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("Failed to read migration: %v", err)
	}

	_, err = db.Exec(ctx, string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute migration: %v", err)
	}
}

func TestAppendEvents(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create test events
	aggregateID := uuid.New().String()
	events := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "TestEventCreated",
			EventVersion:  1,
			Payload:       []byte(`{"test":"data"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "TestEventUpdated",
			EventVersion:  1,
			Payload:       []byte(`{"test":"updated"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Use NoStream() for creating a new aggregate
	result, err := pgStore.Append(ctx, tx, store.NoStream(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	if len(result.GlobalPositions) != len(events) {
		t.Errorf("Expected %d positions, got %d", len(events), len(result.GlobalPositions))
	}

	// Verify positions are sequential
	for i := 1; i < len(result.GlobalPositions); i++ {
		if result.GlobalPositions[i] != result.GlobalPositions[i-1]+1 {
			t.Errorf("Positions not sequential: %v", result.GlobalPositions)
		}
	}

	// Verify persisted events have aggregate versions set
	if len(result.Events) != len(events) {
		t.Errorf("Expected %d persisted events, got %d", len(events), len(result.Events))
	}
	if result.Events[0].AggregateVersion != 1 {
		t.Errorf("Expected first event to have version 1, got %d", result.Events[0].AggregateVersion)
	}
	if result.Events[1].AggregateVersion != 2 {
		t.Errorf("Expected second event to have version 2, got %d", result.Events[1].AggregateVersion)
	}

	// Verify FromVersion and ToVersion
	if result.FromVersion() != 0 {
		t.Errorf("Expected FromVersion=0 for new aggregate, got %d", result.FromVersion())
	}
	if result.ToVersion() != 2 {
		t.Errorf("Expected ToVersion=2, got %d", result.ToVersion())
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
}

func TestAppendEvents_OptimisticConcurrency(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	str := postgres.NewStore(postgres.DefaultStoreConfig())

	aggregateID := uuid.New().String()

	event1 := store.Event{
		AggregateType: "TestAggregate",
		AggregateID:   aggregateID,
		EventID:       uuid.New(),
		EventType:     "TestEventCreated",
		EventVersion:  1,
		Payload:       []byte(`{}`),
		Metadata:      []byte(`{}`),
		CreatedAt:     time.Now(),
	}

	// First, append an event successfully to establish version 1
	tx1, _ := db.BeginTx(ctx, nil)
	_, err := str.Append(ctx, tx1, store.NoStream(), []store.Event{event1})
	if err != nil {
		t.Fatalf("First append failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("First transaction commit failed: %v", err)
	}

	// Now try to manually insert a duplicate version to simulate optimistic concurrency conflict
	// This simulates what happens when two processes both read MAX(version)=1, both try to insert version=2
	event2 := store.Event{
		AggregateType: "TestAggregate",
		AggregateID:   aggregateID,
		EventID:       uuid.New(),
		EventType:     "TestEventUpdated",
		EventVersion:  1,
		Payload:       []byte(`{}`),
		Metadata:      []byte(`{}`),
		CreatedAt:     time.Now(),
	}

	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback(ctx) //nolint:errcheck // cleanup

	// Manually insert with version=1 (which already exists) to trigger unique constraint violation
	_, err = tx2.Exec(ctx, `
		INSERT INTO events (
			aggregate_type, aggregate_id, aggregate_version,
			event_id, event_type, event_version,
			payload, metadata, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, event2.AggregateType, event2.AggregateID, int64(1), // Use version 1 which already exists
		event2.EventID, event2.EventType, event2.EventVersion,
		event2.Payload, string(event2.Metadata), event2.CreatedAt)

	// The insert should fail immediately with unique constraint violation
	if err == nil {
		t.Fatal("Expected unique constraint violation, got nil")
	}

	// Verify it's the right kind of error
	if !postgres.IsUniqueViolation(err) {
		t.Errorf("Expected unique violation error, got: %v", err)
	}
}

func TestReadEvents(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append some events
	aggregateID1 := uuid.New().String()
	aggregateID2 := uuid.New().String()

	events := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID1,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID2,
			EventID:       uuid.New(),
			EventType:     "Event2",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx, store.Any(), events[:1])
	if err != nil {
		t.Fatalf("Failed to append first event: %v", err)
	}
	_, err = pgStore.Append(ctx, tx, store.Any(), events[1:])
	if err != nil {
		t.Fatalf("Failed to append second event: %v", err)
	}
	tx.Commit(ctx)

	// Read events
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback(ctx)

	readEvents, err := pgStore.ReadEvents(ctx, tx2, 0, 10)
	if err != nil {
		t.Fatalf("Failed to read events: %v", err)
	}

	if len(readEvents) != 2 {
		t.Errorf("Expected 2 events, got %d", len(readEvents))
	}

	// Verify ordering
	if readEvents[0].GlobalPosition >= readEvents[1].GlobalPosition {
		t.Error("Events not ordered by global position")
	}
}

func TestReadEvents_RawCheckpointCanSkipEarlierLaterCommittedPosition(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	newEvent := func(aggregateType, eventType string) store.Event {
		return store.Event{
			AggregateType: aggregateType,
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     eventType,
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		}
	}

	txA, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin first transaction: %v", err)
	}
	defer func() {
		_ = txA.Rollback(ctx)
	}()

	resultA, err := pgStore.Append(ctx, txA, store.NoStream(), []store.Event{newEvent("User", "UserCreated")})
	if err != nil {
		t.Fatalf("Failed to append first event: %v", err)
	}

	txB, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}
	defer func() {
		_ = txB.Rollback(ctx)
	}()

	resultB, err := pgStore.Append(ctx, txB, store.NoStream(), []store.Event{newEvent("Order", "OrderPlaced")})
	if err != nil {
		t.Fatalf("Failed to append second event: %v", err)
	}

	posA := resultA.GlobalPositions[0]
	posB := resultB.GlobalPositions[0]
	if posA >= posB {
		t.Fatalf("Expected first transaction to allocate lower position than second: %d >= %d", posA, posB)
	}

	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit second transaction: %v", err)
	}

	readTx1, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("Failed to begin read transaction before first commit: %v", err)
	}
	defer func() {
		_ = readTx1.Rollback(ctx)
	}()

	visibleBeforeFirstCommit, err := pgStore.ReadEvents(ctx, readTx1, 0, 10)
	if err != nil {
		t.Fatalf("Failed to read events before first commit: %v", err)
	}

	if len(visibleBeforeFirstCommit) != 1 {
		t.Fatalf("Expected only the later committed event to be visible, got %d events", len(visibleBeforeFirstCommit))
	}
	if visibleBeforeFirstCommit[0].GlobalPosition != posB {
		t.Fatalf("Expected visible event to have position %d, got %d", posB, visibleBeforeFirstCommit[0].GlobalPosition)
	}

	checkpoint := visibleBeforeFirstCommit[0].GlobalPosition

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit first transaction: %v", err)
	}

	readTx2, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("Failed to begin read transaction after both commits: %v", err)
	}
	defer func() {
		_ = readTx2.Rollback(ctx)
	}()

	eventsAfterCheckpoint, err := pgStore.ReadEvents(ctx, readTx2, checkpoint, 10)
	if err != nil {
		t.Fatalf("Failed to read events after checkpoint: %v", err)
	}

	if len(eventsAfterCheckpoint) != 0 {
		t.Fatalf("Expected no events after checkpoint %d, got %d", checkpoint, len(eventsAfterCheckpoint))
	}

	readTx3, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("Failed to begin verification read transaction: %v", err)
	}
	defer func() {
		_ = readTx3.Rollback(ctx)
	}()

	allEvents, err := pgStore.ReadEvents(ctx, readTx3, 0, 10)
	if err != nil {
		t.Fatalf("Failed to read all events after both commits: %v", err)
	}

	if len(allEvents) != 2 {
		t.Fatalf("Expected both events to be visible after both commits, got %d", len(allEvents))
	}
	if allEvents[0].GlobalPosition != posA || allEvents[1].GlobalPosition != posB {
		t.Fatalf("Expected final visible ordering [%d, %d], got [%d, %d]",
			posA, posB, allEvents[0].GlobalPosition, allEvents[1].GlobalPosition)
	}
}

func TestReadEvents_RawPositionsCanContainPermanentGapAfterRollback(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	newEvent := func(aggregateType, eventType string) store.Event {
		return store.Event{
			AggregateType: aggregateType,
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     eventType,
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		}
	}

	txRolledBack, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin rollback transaction: %v", err)
	}
	defer func() {
		_ = txRolledBack.Rollback(ctx)
	}()

	rolledBackResult, err := pgStore.Append(ctx, txRolledBack, store.NoStream(), []store.Event{newEvent("User", "UserCreated")})
	if err != nil {
		t.Fatalf("Failed to append rolled back event: %v", err)
	}

	txCommitted, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin committed transaction: %v", err)
	}
	defer func() {
		_ = txCommitted.Rollback(ctx)
	}()

	committedResult, err := pgStore.Append(ctx, txCommitted, store.NoStream(), []store.Event{newEvent("Order", "OrderPlaced")})
	if err != nil {
		t.Fatalf("Failed to append committed event: %v", err)
	}

	rolledBackPos := rolledBackResult.GlobalPositions[0]
	committedPos := committedResult.GlobalPositions[0]
	if rolledBackPos >= committedPos {
		t.Fatalf("Expected rolled back transaction to allocate lower position than committed one: %d >= %d",
			rolledBackPos, committedPos)
	}

	if err := txRolledBack.Rollback(ctx); err != nil {
		t.Fatalf("Failed to roll back first transaction: %v", err)
	}
	if err := txCommitted.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit second transaction: %v", err)
	}

	readTx, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer func() {
		_ = readTx.Rollback(ctx)
	}()

	visibleEvents, err := pgStore.ReadEvents(ctx, readTx, 0, 10)
	if err != nil {
		t.Fatalf("Failed to read visible events: %v", err)
	}

	if len(visibleEvents) != 1 {
		t.Fatalf("Expected one committed event after rollback, got %d", len(visibleEvents))
	}
	if visibleEvents[0].GlobalPosition != committedPos {
		t.Fatalf("Expected committed event to have position %d, got %d", committedPos, visibleEvents[0].GlobalPosition)
	}

	var count int
	if err := readTx.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE global_position = $1`, rolledBackPos).Scan(&count); err != nil {
		t.Fatalf("Failed to check rolled back position visibility: %v", err)
	}
	if count != 0 {
		t.Fatalf("Expected rolled back position %d to be absent, count=%d", rolledBackPos, count)
	}

	latestPosition, err := pgStore.GetLatestGlobalPosition(ctx, readTx)
	if err != nil {
		t.Fatalf("Failed to read latest global position: %v", err)
	}
	if latestPosition != committedPos {
		t.Fatalf("Expected latest global position to be %d, got %d", committedPos, latestPosition)
	}
}

func TestReadEvents_Pagination(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append multiple events
	for i := 0; i < 5; i++ {
		event := store.Event{
			AggregateType: "TestAggregate",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     fmt.Sprintf("Event%d", i),
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		}

		tx, _ := db.BeginTx(ctx, nil)
		_, err := pgStore.Append(ctx, tx, store.Any(), []store.Event{event})
		if err != nil {
			t.Fatalf("Failed to append event: %v", err)
		}
		tx.Commit(ctx)
	}

	// Read first batch
	tx1, _ := db.BeginTx(ctx, nil)
	defer tx1.Rollback(ctx)

	batch1, err := pgStore.ReadEvents(ctx, tx1, 0, 2)
	if err != nil {
		t.Fatalf("Failed to read first batch: %v", err)
	}

	if len(batch1) != 2 {
		t.Errorf("Expected 2 events in first batch, got %d", len(batch1))
	}

	// Read second batch
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback(ctx)

	batch2, err := pgStore.ReadEvents(ctx, tx2, batch1[len(batch1)-1].GlobalPosition, 2)
	if err != nil {
		t.Fatalf("Failed to read second batch: %v", err)
	}

	if len(batch2) != 2 {
		t.Errorf("Expected 2 events in second batch, got %d", len(batch2))
	}

	// Verify no overlap
	for _, e1 := range batch1 {
		for _, e2 := range batch2 {
			if e1.GlobalPosition == e2.GlobalPosition {
				t.Error("Batches have overlapping events")
			}
		}
	}
}

func TestAggregateVersionTracking(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	aggregateID := uuid.New().String()

	// Append first batch of events
	events1 := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event2",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.Any(), events1)
	if err != nil {
		t.Fatalf("First append failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Verify aggregate_heads has correct version
	var aggVersion int64
	err = db.QueryRowContext(ctx, `
		SELECT aggregate_version 
		FROM aggregate_heads 
		WHERE aggregate_type = $1 AND aggregate_id = $2
	`, "TestAggregate", aggregateID).Scan(&aggVersion)
	if err != nil {
		t.Fatalf("Failed to query aggregate_heads: %v", err)
	}
	if aggVersion != 2 {
		t.Errorf("Expected aggregate version 2, got %d", aggVersion)
	}

	// Append second batch of events
	events2 := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event3",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.Any(), events2)
	if err != nil {
		t.Fatalf("Second append failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Second commit failed: %v", err)
	}

	// Verify aggregate_heads was updated
	err = db.QueryRowContext(ctx, `
		SELECT aggregate_version 
		FROM aggregate_heads 
		WHERE aggregate_type = $1 AND aggregate_id = $2
	`, "TestAggregate", aggregateID).Scan(&aggVersion)
	if err != nil {
		t.Fatalf("Failed to query aggregate_heads: %v", err)
	}
	if aggVersion != 3 {
		t.Errorf("Expected aggregate version 3, got %d", aggVersion)
	}

	// Verify events have correct versions
	rows, err := db.QueryContext(ctx, `
		SELECT aggregate_version 
		FROM events 
		WHERE aggregate_type = $1 AND aggregate_id = $2 
		ORDER BY aggregate_version
	`, "TestAggregate", aggregateID)
	if err != nil {
		t.Fatalf("Failed to query events: %v", err)
	}
	defer rows.Close()

	expectedVersions := []int64{1, 2, 3}
	var versions []int64
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			t.Fatalf("Failed to scan version: %v", err)
		}
		versions = append(versions, version)
	}

	if len(versions) != len(expectedVersions) {
		t.Errorf("Expected %d events, got %d", len(expectedVersions), len(versions))
	}

	for i, expected := range expectedVersions {
		if i >= len(versions) {
			break
		}
		if versions[i] != expected {
			t.Errorf("Event %d: expected version %d, got %d", i, expected, versions[i])
		}
	}
}

func TestAggregateVersionTracking_MultipleAggregates(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create events for two different aggregates
	aggregate1 := uuid.New().String()
	aggregate2 := uuid.New().String()

	events1 := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregate1,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	events2 := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregate2,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	// Append events for both aggregates
	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.Any(), events1)
	if err != nil {
		t.Fatalf("Failed to append events for aggregate1: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit aggregate1: %v", err)
	}

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.Any(), events2)
	if err != nil {
		t.Fatalf("Failed to append events for aggregate2: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit aggregate2: %v", err)
	}

	// Verify both aggregates have version 1
	var version1, version2 int64
	err = db.QueryRowContext(ctx, `
		SELECT aggregate_version 
		FROM aggregate_heads 
		WHERE aggregate_type = $1 AND aggregate_id = $2
	`, "TestAggregate", aggregate1).Scan(&version1)
	if err != nil {
		t.Fatalf("Failed to query version for aggregate1: %v", err)
	}

	err = db.QueryRowContext(ctx, `
		SELECT aggregate_version 
		FROM aggregate_heads 
		WHERE aggregate_type = $1 AND aggregate_id = $2
	`, "TestAggregate", aggregate2).Scan(&version2)
	if err != nil {
		t.Fatalf("Failed to query version for aggregate2: %v", err)
	}

	if version1 != 1 {
		t.Errorf("Expected aggregate1 version 1, got %d", version1)
	}
	if version2 != 1 {
		t.Errorf("Expected aggregate2 version 1, got %d", version2)
	}

	// Verify aggregate_heads has exactly 2 rows
	var count int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM aggregate_heads`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count aggregate_heads: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows in aggregate_heads, got %d", count)
	}
}

func TestReadAggregateStream_FullStream(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create test events for one aggregate
	aggregateID := uuid.New().String()
	events := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{"data":"1"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event2",
			EventVersion:  1,
			Payload:       []byte(`{"data":"2"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event3",
			EventVersion:  1,
			Payload:       []byte(`{"data":"3"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx, store.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit(ctx)

	// Read full stream
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback(ctx)

	stream, err := pgStore.ReadAggregateStream(ctx, tx2, "TestAggregate", aggregateID, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read aggregate stream: %v", err)
	}

	if stream.Len() != 3 {
		t.Errorf("Expected 3 events, got %d", stream.Len())
	}

	// Verify stream version
	if stream.Version() != 3 {
		t.Errorf("Expected stream version 3, got %d", stream.Version())
	}

	// Verify events are ordered by aggregate_version
	for i, event := range stream.Events {
		expectedVersion := int64(i + 1)
		if event.AggregateVersion != expectedVersion {
			t.Errorf("Event %d: expected version %d, got %d", i, expectedVersion, event.AggregateVersion)
		}
		if event.AggregateID != aggregateID {
			t.Errorf("Event %d: wrong aggregate ID", i)
		}
		if event.AggregateType != "TestAggregate" {
			t.Errorf("Event %d: wrong aggregate type", i)
		}
	}
}

func TestReadAggregateStream_WithFromVersion(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create test events
	aggregateID := uuid.New().String()
	events := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event2",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event3",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx, store.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit(ctx)

	// Read from version 2 onwards
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback(ctx)

	fromVersion := int64(2)
	stream, err := pgStore.ReadAggregateStream(ctx, tx2, "TestAggregate", aggregateID, &fromVersion, nil)
	if err != nil {
		t.Fatalf("Failed to read aggregate stream: %v", err)
	}

	if stream.Len() != 2 {
		t.Errorf("Expected 2 events, got %d", stream.Len())
	}

	// Verify stream version (should be the last event's version)
	if stream.Version() != 3 {
		t.Errorf("Expected stream version 3, got %d", stream.Version())
	}

	// Verify we got versions 2 and 3
	if len(stream.Events) > 0 && stream.Events[0].AggregateVersion != 2 {
		t.Errorf("First event: expected version 2, got %d", stream.Events[0].AggregateVersion)
	}
	if len(stream.Events) > 1 && stream.Events[1].AggregateVersion != 3 {
		t.Errorf("Second event: expected version 3, got %d", stream.Events[1].AggregateVersion)
	}
}

func TestReadAggregateStream_WithToVersion(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create test events
	aggregateID := uuid.New().String()
	events := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event2",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event3",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx, store.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit(ctx)

	// Read up to version 2
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback(ctx)

	toVersion := int64(2)
	stream, err := pgStore.ReadAggregateStream(ctx, tx2, "TestAggregate", aggregateID, nil, &toVersion)
	if err != nil {
		t.Fatalf("Failed to read aggregate stream: %v", err)
	}

	if stream.Len() != 2 {
		t.Errorf("Expected 2 events, got %d", stream.Len())
	}

	// Verify stream version (should be 2 since we read up to version 2)
	if stream.Version() != 2 {
		t.Errorf("Expected stream version 2, got %d", stream.Version())
	}

	// Verify we got versions 1 and 2
	if len(stream.Events) > 0 && stream.Events[0].AggregateVersion != 1 {
		t.Errorf("First event: expected version 1, got %d", stream.Events[0].AggregateVersion)
	}
	if len(stream.Events) > 1 && stream.Events[1].AggregateVersion != 2 {
		t.Errorf("Second event: expected version 2, got %d", stream.Events[1].AggregateVersion)
	}
}

func TestReadAggregateStream_WithVersionRange(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create test events
	aggregateID := uuid.New().String()
	events := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event2",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event3",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event4",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx, store.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit(ctx)

	// Read versions 2-3
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback(ctx)

	fromVersion := int64(2)
	toVersion := int64(3)
	stream, err := pgStore.ReadAggregateStream(ctx, tx2, "TestAggregate", aggregateID, &fromVersion, &toVersion)
	if err != nil {
		t.Fatalf("Failed to read aggregate stream: %v", err)
	}

	if stream.Len() != 2 {
		t.Errorf("Expected 2 events, got %d", stream.Len())
	}

	// Verify stream version (should be 3 since we read up to version 3)
	if stream.Version() != 3 {
		t.Errorf("Expected stream version 3, got %d", stream.Version())
	}

	// Verify we got versions 2 and 3
	if len(stream.Events) > 0 && stream.Events[0].AggregateVersion != 2 {
		t.Errorf("First event: expected version 2, got %d", stream.Events[0].AggregateVersion)
	}
	if len(stream.Events) > 1 && stream.Events[1].AggregateVersion != 3 {
		t.Errorf("Second event: expected version 3, got %d", stream.Events[1].AggregateVersion)
	}
}

func TestReadAggregateStream_EmptyResult(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Don't append any events

	// Try to read non-existent aggregate
	tx, _ := db.BeginTx(ctx, nil)
	defer tx.Rollback(ctx)

	nonExistentID := uuid.New().String()
	stream, err := pgStore.ReadAggregateStream(ctx, tx, "TestAggregate", nonExistentID, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read aggregate stream: %v", err)
	}

	if !stream.IsEmpty() {
		t.Errorf("Expected empty stream, got %d events", stream.Len())
	}

	if stream.Version() != 0 {
		t.Errorf("Expected version 0 for empty stream, got %d", stream.Version())
	}
}

func TestReadAggregateStream_MultipleAggregates(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create events for two aggregates
	aggregate1 := uuid.New().String()
	aggregate2 := uuid.New().String()

	events1 := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregate1,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregate1,
			EventID:       uuid.New(),
			EventType:     "Event2",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	events2 := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregate2,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.Any(), events1)
	if err != nil {
		t.Fatalf("Failed to append events for aggregate1: %v", err)
	}
	tx1.Commit(ctx)

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.Any(), events2)
	if err != nil {
		t.Fatalf("Failed to append events for aggregate2: %v", err)
	}
	tx2.Commit(ctx)

	// Read aggregate1 stream
	tx3, _ := db.BeginTx(ctx, nil)
	defer tx3.Rollback(ctx)

	stream1, err := pgStore.ReadAggregateStream(ctx, tx3, "TestAggregate", aggregate1, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read aggregate1 stream: %v", err)
	}

	if stream1.Len() != 2 {
		t.Errorf("Expected 2 events for aggregate1, got %d", stream1.Len())
	}

	// Read aggregate2 stream
	stream2, err := pgStore.ReadAggregateStream(ctx, tx3, "TestAggregate", aggregate2, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read aggregate2 stream: %v", err)
	}

	if stream2.Len() != 1 {
		t.Errorf("Expected 1 event for aggregate2, got %d", stream2.Len())
	}

	// Verify no cross-contamination
	for _, e := range stream1.Events {
		if e.AggregateID != aggregate1 {
			t.Error("aggregate1 stream contains event from different aggregate")
		}
	}
	for _, e := range stream2.Events {
		if e.AggregateID != aggregate2 {
			t.Error("aggregate2 stream contains event from different aggregate")
		}
	}
}

func TestReadAggregateStream_Ordering(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create events in batches to ensure ordering is by aggregate_version not global_position
	aggregateID := uuid.New().String()

	// First batch
	events1 := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event1",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.Any(), events1)
	if err != nil {
		t.Fatalf("Failed to append first batch: %v", err)
	}
	tx1.Commit(ctx)

	// Append event for different aggregate in between
	otherAggregate := uuid.New().String()
	eventsOther := []store.Event{
		{
			AggregateType: "OtherAggregate",
			AggregateID:   otherAggregate,
			EventID:       uuid.New(),
			EventType:     "OtherEvent",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.Any(), eventsOther)
	if err != nil {
		t.Fatalf("Failed to append other event: %v", err)
	}
	tx2.Commit(ctx)

	// Second batch for our aggregate
	events2 := []store.Event{
		{
			AggregateType: "TestAggregate",
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     "Event2",
			EventVersion:  1,
			Payload:       []byte(`{}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx3, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx3, store.Any(), events2)
	if err != nil {
		t.Fatalf("Failed to append second batch: %v", err)
	}
	tx3.Commit(ctx)

	// Read the stream
	tx4, _ := db.BeginTx(ctx, nil)
	defer tx4.Rollback(ctx)

	stream, err := pgStore.ReadAggregateStream(ctx, tx4, "TestAggregate", aggregateID, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read aggregate stream: %v", err)
	}

	if stream.Len() != 2 {
		t.Errorf("Expected 2 events, got %d", stream.Len())
	}

	// Verify stream version
	if stream.Version() != 2 {
		t.Errorf("Expected stream version 2, got %d", stream.Version())
	}

	// Verify ordering by aggregate_version (should be 1, 2)
	for i, event := range stream.Events {
		expectedVersion := int64(i + 1)
		if event.AggregateVersion != expectedVersion {
			t.Errorf("Event %d: expected version %d, got %d", i, expectedVersion, event.AggregateVersion)
		}
	}

	// Verify global positions are NOT necessarily sequential (due to interleaved aggregate)
	if len(stream.Events) == 2 {
		if stream.Events[1].GlobalPosition == stream.Events[0].GlobalPosition+1 {
			// This might happen, but we're just documenting that ordering is by aggregate_version
			t.Log("Note: global positions happen to be sequential, but ordering is guaranteed by aggregate_version")
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────

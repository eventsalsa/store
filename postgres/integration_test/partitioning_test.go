//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/eventsalsa/store"
	"github.com/eventsalsa/store/migrations"
	"github.com/eventsalsa/store/postgres"
)

// setupPartitionedTestTables sets up a partitioned PostgreSQL event store with small partition size.
func setupPartitionedTestTables(t *testing.T, db testDB, partitionSize int64, initialPartitions int) {
	t.Helper()
	ctx := context.Background()

	// Drop existing objects
	_, err := db.Exec(ctx, `
		DROP TABLE IF EXISTS stream_heads CASCADE;
		DROP TABLE IF EXISTS events CASCADE;
		DROP SEQUENCE IF EXISTS events_global_position_seq CASCADE;
	`)
	if err != nil {
		t.Fatalf("Failed to drop tables: %v", err)
	}

	tmpDir := t.TempDir()
	config := &migrations.Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "partitioned_test.sql",
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
		Partitioning: migrations.PartitionConfig{
			Strategy:          migrations.PartitionStrategyNative,
			PartitionSize:     partitionSize,
			InitialPartitions: initialPartitions,
		},
	}

	if err := migrations.GeneratePostgres(config); err != nil {
		t.Fatalf("Failed to generate partitioned migration: %v", err)
	}

	migrationSQL, err := os.ReadFile(fmt.Sprintf("%s/%s", tmpDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("Failed to read migration: %v", err)
	}

	_, err = db.Exec(ctx, string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute partitioned migration: %v", err)
	}
}

// TestPartitioned_BoundaryCrossing verifies that events appended across partition boundaries
// physically reside in their respective child partition tables.
func TestPartitioned_BoundaryCrossing(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	// 5 events per partition, 3 partitions (1..5, 6..10, 11..15)
	setupPartitionedTestTables(t, db, 5, 3)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	streamID := uuid.New().String()
	streamType := "Order"

	// Append 15 events in batches of 3
	for b := 0; b < 5; b++ {
		var events []store.Event
		for i := 0; i < 3; i++ {
			events = append(events, store.Event{
				StreamType:   streamType,
				StreamID:     streamID,
				EventID:      uuid.New(),
				EventType:    "OrderUpdated",
				EventVersion: 1,
				Payload:      []byte(fmt.Sprintf(`{"batch": %d, "idx": %d}`, b, i)),
				CreatedAt:    time.Now(),
			})
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin tx: %v", err)
		}

		var exp store.ExpectedVersion
		if b == 0 {
			exp = store.NoStream()
		} else {
			exp = store.Exact(int64(b * 3))
		}

		_, err = pgStore.Append(ctx, tx, exp, events)
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("Append batch %d failed: %v", b, err)
		}

		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("Commit batch %d failed: %v", b, err)
		}
	}

	// Directly check counts in physical child partition tables
	var countP1, countP2, countP3 int
	err := db.QueryRow(ctx, "SELECT count(*) FROM events_p0000000001_p0000000005").Scan(&countP1)
	if err != nil {
		t.Fatalf("Failed to query partition 1: %v", err)
	}
	err = db.QueryRow(ctx, "SELECT count(*) FROM events_p0000000006_p0000000010").Scan(&countP2)
	if err != nil {
		t.Fatalf("Failed to query partition 2: %v", err)
	}
	err = db.QueryRow(ctx, "SELECT count(*) FROM events_p0000000011_p0000000015").Scan(&countP3)
	if err != nil {
		t.Fatalf("Failed to query partition 3: %v", err)
	}

	if countP1 != 5 {
		t.Errorf("Partition 1 expected 5 rows, got %d", countP1)
	}
	if countP2 != 5 {
		t.Errorf("Partition 2 expected 5 rows, got %d", countP2)
	}
	if countP3 != 5 {
		t.Errorf("Partition 3 expected 5 rows, got %d", countP3)
	}
}

// TestPartitioned_BatchStraddlingBoundary verifies that a single atomic Append transaction
// can insert events that span across a partition boundary.
func TestPartitioned_BatchStraddlingBoundary(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	// 5 events per partition
	setupPartitionedTestTables(t, db, 5, 3)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	streamID := uuid.New().String()
	streamType := "Shipment"

	// Append initial 3 events (lands in p1: pos 1, 2, 3)
	var initEvents []store.Event
	for i := 0; i < 3; i++ {
		initEvents = append(initEvents, store.Event{
			StreamType:   streamType,
			StreamID:     streamID,
			EventID:      uuid.New(),
			EventType:    "ShipmentItemAdded",
			EventVersion: 1,
			Payload:      []byte(`{}`),
			CreatedAt:    time.Now(),
		})
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.NoStream(), initEvents)
	if err != nil {
		t.Fatalf("Initial append failed: %v", err)
	}
	_ = tx1.Commit(ctx)

	// Append batch of 4 events: will allocate pos 4, 5 (in p1) and pos 6, 7 (in p2)
	var straddlingEvents []store.Event
	for i := 0; i < 4; i++ {
		straddlingEvents = append(straddlingEvents, store.Event{
			StreamType:   streamType,
			StreamID:     streamID,
			EventID:      uuid.New(),
			EventType:    "ShipmentUpdated",
			EventVersion: 1,
			Payload:      []byte(fmt.Sprintf(`{"idx": %d}`, i)),
			CreatedAt:    time.Now(),
		})
	}

	tx2, _ := db.BeginTx(ctx, nil)
	res, err := pgStore.Append(ctx, tx2, store.Exact(3), straddlingEvents)
	if err != nil {
		t.Fatalf("Straddling append failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit straddling append failed: %v", err)
	}

	if res.FromVersion() != 3 || res.ToVersion() != 7 {
		t.Errorf("Expected version range 3..7, got %d..%d", res.FromVersion(), res.ToVersion())
	}

	// Verify partition row distribution
	var countP1, countP2 int
	_ = db.QueryRow(ctx, "SELECT count(*) FROM events_p0000000001_p0000000005").Scan(&countP1)
	_ = db.QueryRow(ctx, "SELECT count(*) FROM events_p0000000006_p0000000010").Scan(&countP2)

	if countP1 != 5 {
		t.Errorf("Partition 1 expected 5 rows (3 + 2), got %d", countP1)
	}
	if countP2 != 2 {
		t.Errorf("Partition 2 expected 2 rows, got %d", countP2)
	}
}

// TestPartitioned_ReadStream_AcrossPartitions verifies that ReadStream reads complete stream
// histories correctly across multiple partitions, including version slice queries.
func TestPartitioned_ReadStream_AcrossPartitions(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	// 5 events per partition, 3 partitions
	setupPartitionedTestTables(t, db, 5, 3)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	streamID := uuid.New().String()
	streamType := "UserProfile"

	// Append 12 events total (spanning p1, p2, p3)
	for i := 1; i <= 12; i++ {
		event := store.Event{
			StreamType:   streamType,
			StreamID:     streamID,
			EventID:      uuid.New(),
			EventType:    "ProfileUpdated",
			EventVersion: 1,
			Payload:      []byte(fmt.Sprintf(`{"step": %d}`, i)),
			CreatedAt:    time.Now(),
		}

		tx, _ := db.BeginTx(ctx, nil)
		var exp store.ExpectedVersion
		if i == 1 {
			exp = store.NoStream()
		} else {
			exp = store.Exact(int64(i - 1))
		}

		_, err := pgStore.Append(ctx, tx, exp, []store.Event{event})
		if err != nil {
			t.Fatalf("Append event %d failed: %v", i, err)
		}
		_ = tx.Commit(ctx)
	}

	txRead, _ := db.BeginTx(ctx, nil)
	defer txRead.Rollback(ctx)

	// 1. Full stream read across all 3 partitions
	fullStream, err := pgStore.ReadStream(ctx, txRead, streamType, streamID, nil, nil)
	if err != nil {
		t.Fatalf("ReadStream full failed: %v", err)
	}
	if len(fullStream.Events) != 12 {
		t.Fatalf("Expected 12 events, got %d", len(fullStream.Events))
	}
	if fullStream.Version() != 12 {
		t.Fatalf("Expected stream version 12, got %d", fullStream.Version())
	}

	for i, e := range fullStream.Events {
		expectedVersion := int64(i + 1)
		if e.StreamVersion != expectedVersion {
			t.Errorf("Event %d: expected version %d, got %d", i, expectedVersion, e.StreamVersion)
		}
	}

	// 2. Version range query spanning partitions (versions 4 to 9 spans p1 and p2)
	fromV := int64(4)
	toV := int64(9)
	rangeStream, err := pgStore.ReadStream(ctx, txRead, streamType, streamID, &fromV, &toV)
	if err != nil {
		t.Fatalf("ReadStream range failed: %v", err)
	}
	if len(rangeStream.Events) != 6 {
		t.Fatalf("Expected 6 events in range 4..9, got %d", len(rangeStream.Events))
	}
	if rangeStream.Events[0].StreamVersion != 4 || rangeStream.Events[5].StreamVersion != 9 {
		t.Errorf("Expected versions 4..9, got %d..%d", rangeStream.Events[0].StreamVersion, rangeStream.Events[5].StreamVersion)
	}
}

// TestPartitioned_ReadEvents_Pagination verifies that ReadEvents sequentially paginates
// events across partition boundaries without dropping or repeating records.
func TestPartitioned_ReadEvents_Pagination(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	// 5 events per partition, 3 partitions
	setupPartitionedTestTables(t, db, 5, 3)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	streamType := "Feed"
	totalEvents := 15

	// Append 15 events
	for i := 0; i < totalEvents; i++ {
		event := store.Event{
			StreamType:   streamType,
			StreamID:     uuid.New().String(),
			EventID:      uuid.New(),
			EventType:    "FeedItemCreated",
			EventVersion: 1,
			Payload:      []byte(fmt.Sprintf(`{"item": %d}`, i)),
			CreatedAt:    time.Now(),
		}

		tx, _ := db.BeginTx(ctx, nil)
		_, err := pgStore.Append(ctx, tx, store.NoStream(), []store.Event{event})
		if err != nil {
			t.Fatalf("Append feed event %d failed: %v", i, err)
		}
		_ = tx.Commit(ctx)
	}

	// Paginate in batches of 4
	txRead, _ := db.BeginTx(ctx, nil)
	defer txRead.Rollback(ctx)

	var allEvents []store.PersistedEvent
	currentPos := int64(0)
	pageSize := 4

	for {
		batch, err := pgStore.ReadEvents(ctx, txRead, currentPos, pageSize)
		if err != nil {
			t.Fatalf("ReadEvents at pos %d failed: %v", currentPos, err)
		}
		if len(batch) == 0 {
			break
		}
		allEvents = append(allEvents, batch...)
		currentPos = batch[len(batch)-1].GlobalPosition
	}

	if len(allEvents) != totalEvents {
		t.Fatalf("Expected %d paginated events, got %d", totalEvents, len(allEvents))
	}

	for i, e := range allEvents {
		expectedPos := int64(i + 1)
		if e.GlobalPosition != expectedPos {
			t.Errorf("Event %d: expected GlobalPosition %d, got %d", i, expectedPos, e.GlobalPosition)
		}
	}
}

// TestPartitioned_GetLatestGlobalPosition verifies latest position lookup on partitioned table.
func TestPartitioned_GetLatestGlobalPosition(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	// 5 events per partition, 3 partitions
	setupPartitionedTestTables(t, db, 5, 3)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	txRead, _ := db.BeginTx(ctx, nil)
	pos0, err := pgStore.GetLatestGlobalPosition(ctx, txRead)
	if err != nil {
		t.Fatalf("GetLatestGlobalPosition on empty store failed: %v", err)
	}
	if pos0 != 0 {
		t.Errorf("Expected 0 on empty store, got %d", pos0)
	}
	_ = txRead.Rollback(ctx)

	// Append 8 events (crosses into partition 2)
	for i := 0; i < 8; i++ {
		event := store.Event{
			StreamType:   "Metric",
			StreamID:     "m1",
			EventID:      uuid.New(),
			EventType:    "MetricLogged",
			EventVersion: 1,
			Payload:      []byte(`{}`),
			CreatedAt:    time.Now(),
		}
		tx, _ := db.BeginTx(ctx, nil)
		_, err := pgStore.Append(ctx, tx, store.Any(), []store.Event{event})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		_ = tx.Commit(ctx)
	}

	txRead2, _ := db.BeginTx(ctx, nil)
	defer txRead2.Rollback(ctx)

	pos8, err := pgStore.GetLatestGlobalPosition(ctx, txRead2)
	if err != nil {
		t.Fatalf("GetLatestGlobalPosition failed: %v", err)
	}
	if pos8 != 8 {
		t.Errorf("Expected latest position 8, got %d", pos8)
	}
}

// TestPartitioned_MissingPartition_Error verifies proper error reporting when an append
// attempts to write beyond available partition ranges.
func TestPartitioned_MissingPartition_Error(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	// 5 events per partition, only 1 partition (values from 1 to 6)
	setupPartitionedTestTables(t, db, 5, 1)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append 5 events (fills partition 1)
	var events []store.Event
	for i := 0; i < 5; i++ {
		events = append(events, store.Event{
			StreamType:   "Log",
			StreamID:     "l1",
			EventID:      uuid.New(),
			EventType:    "LogAdded",
			EventVersion: 1,
			Payload:      []byte(`{}`),
			CreatedAt:    time.Now(),
		})
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.NoStream(), events)
	if err != nil {
		t.Fatalf("Append 5 events failed: %v", err)
	}
	_ = tx1.Commit(ctx)

	// 6th event has global_position = 6, which falls outside partition [1, 6)
	event6 := store.Event{
		StreamType:   "Log",
		StreamID:     "l1",
		EventID:      uuid.New(),
		EventType:    "LogAdded",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.Exact(5), []store.Event{event6})
	if err == nil {
		_ = tx2.Commit(ctx)
		t.Fatal("Expected append beyond partition range to fail, but it succeeded")
	}
	_ = tx2.Rollback(ctx)
}

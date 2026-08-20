//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/eventsalsa/store"
	"github.com/eventsalsa/store/postgres"
)

// TestConcurrent_NoStream verifies that when multiple concurrent goroutines
// attempt to create the same stream with NoStream(), exactly one succeeds and
// all others fail with ErrOptimisticConcurrency.
func TestConcurrent_NoStream(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := uuid.New().String()
	streamType := "User"

	concurrency := 50
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var conflictCount atomic.Int32
	var otherErrCount atomic.Int32

	startGate := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := context.Background()

			event := store.Event{
				StreamType:   streamType,
				StreamID:     streamID,
				EventID:      uuid.New(),
				EventType:    "UserCreated",
				EventVersion: 1,
				Payload:      []byte(fmt.Sprintf(`{"user_num": %d}`, idx)),
				CreatedAt:    time.Now(),
			}

			// Wait for all goroutines to be ready
			<-startGate

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				otherErrCount.Add(1)
				return
			}

			_, err = pgStore.Append(ctx, tx, store.NoStream(), []store.Event{event})
			if err != nil {
				_ = tx.Rollback(ctx)
				if err == store.ErrOptimisticConcurrency {
					conflictCount.Add(1)
				} else {
					otherErrCount.Add(1)
				}
				return
			}

			if err := tx.Commit(ctx); err != nil {
				otherErrCount.Add(1)
				return
			}

			successCount.Add(1)
		}(i)
	}

	// Release all goroutines simultaneously
	close(startGate)
	wg.Wait()

	if successCount.Load() != 1 {
		t.Fatalf("Expected exactly 1 success for concurrent NoStream(), got %d", successCount.Load())
	}
	if conflictCount.Load() != int32(concurrency-1) {
		t.Fatalf("Expected %d conflicts, got %d (other errors: %d)", concurrency-1, conflictCount.Load(), otherErrCount.Load())
	}

	// Verify stream state in DB
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin read tx: %v", err)
	}
	defer tx.Rollback(ctx)

	s, err := pgStore.ReadStream(ctx, tx, streamType, streamID, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read stream: %v", err)
	}
	if len(s.Events) != 1 {
		t.Fatalf("Expected exactly 1 event in stream, got %d", len(s.Events))
	}
	if s.Version() != 1 {
		t.Fatalf("Expected stream version 1, got %d", s.Version())
	}
}

// TestConcurrent_Exact_Zero verifies that Exact(0) acts identically to NoStream()
// under concurrent execution.
func TestConcurrent_Exact_Zero(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := uuid.New().String()
	streamType := "Account"

	concurrency := 50
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var conflictCount atomic.Int32

	startGate := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := context.Background()

			event := store.Event{
				StreamType:   streamType,
				StreamID:     streamID,
				EventID:      uuid.New(),
				EventType:    "AccountOpened",
				EventVersion: 1,
				Payload:      []byte(`{}`),
				CreatedAt:    time.Now(),
			}

			<-startGate

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return
			}

			_, err = pgStore.Append(ctx, tx, store.Exact(0), []store.Event{event})
			if err != nil {
				_ = tx.Rollback(ctx)
				if err == store.ErrOptimisticConcurrency {
					conflictCount.Add(1)
				}
				return
			}

			if err := tx.Commit(ctx); err == nil {
				successCount.Add(1)
			}
		}(i)
	}

	close(startGate)
	wg.Wait()

	if successCount.Load() != 1 {
		t.Fatalf("Expected exactly 1 success for concurrent Exact(0), got %d", successCount.Load())
	}
	if conflictCount.Load() != int32(concurrency-1) {
		t.Fatalf("Expected %d conflicts, got %d", concurrency-1, conflictCount.Load())
	}
}

// TestConcurrent_Exact_ExistingVersion verifies that when multiple goroutines attempt
// to append to an existing stream at Exact(N), exactly one succeeds.
func TestConcurrent_Exact_ExistingVersion(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := uuid.New().String()
	streamType := "Order"

	// Create initial stream at version 1
	initialEvent := store.Event{
		StreamType:   streamType,
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "OrderPlaced",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	txInit, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, txInit, store.NoStream(), []store.Event{initialEvent})
	if err != nil {
		t.Fatalf("Failed to append initial event: %v", err)
	}
	_ = txInit.Commit(ctx)

	// Attempt 50 concurrent appends expecting version 1
	concurrency := 50
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var conflictCount atomic.Int32

	startGate := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c := context.Background()

			event := store.Event{
				StreamType:   streamType,
				StreamID:     streamID,
				EventID:      uuid.New(),
				EventType:    "OrderItemAdded",
				EventVersion: 1,
				Payload:      []byte(fmt.Sprintf(`{"item_num": %d}`, idx)),
				CreatedAt:    time.Now(),
			}

			<-startGate

			tx, err := db.BeginTx(c, nil)
			if err != nil {
				return
			}

			_, err = pgStore.Append(c, tx, store.Exact(1), []store.Event{event})
			if err != nil {
				_ = tx.Rollback(c)
				if err == store.ErrOptimisticConcurrency {
					conflictCount.Add(1)
				}
				return
			}

			if err := tx.Commit(c); err == nil {
				successCount.Add(1)
			}
		}(i)
	}

	close(startGate)
	wg.Wait()

	if successCount.Load() != 1 {
		t.Fatalf("Expected exactly 1 success for concurrent Exact(1), got %d", successCount.Load())
	}
	if conflictCount.Load() != int32(concurrency-1) {
		t.Fatalf("Expected %d conflicts, got %d", concurrency-1, conflictCount.Load())
	}

	// Verify stream is now at version 2 with exactly 2 events
	txCheck, _ := db.BeginTx(ctx, nil)
	defer txCheck.Rollback(ctx)

	s, err := pgStore.ReadStream(ctx, txCheck, streamType, streamID, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read stream: %v", err)
	}
	if len(s.Events) != 2 {
		t.Fatalf("Expected 2 events in stream, got %d", len(s.Events))
	}
	if s.Version() != 2 {
		t.Fatalf("Expected stream version 2, got %d", s.Version())
	}
}

// TestConcurrent_Any_Unconditional verifies that concurrent appends with store.Any()
// all succeed without conflict, assigning unique consecutive stream versions.
func TestConcurrent_Any_Unconditional(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := uuid.New().String()
	streamType := "SensorLog"

	concurrency := 50
	eventsPerGoroutine := 3
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32

	startGate := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := context.Background()

			var events []store.Event
			for j := 0; j < eventsPerGoroutine; j++ {
				events = append(events, store.Event{
					StreamType:   streamType,
					StreamID:     streamID,
					EventID:      uuid.New(),
					EventType:    "ReadingLogged",
					EventVersion: 1,
					Payload:      []byte(fmt.Sprintf(`{"g": %d, "e": %d}`, idx, j)),
					CreatedAt:    time.Now(),
				})
			}

			<-startGate

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				errorCount.Add(1)
				return
			}

			result, err := pgStore.Append(ctx, tx, store.Any(), events)
			if err != nil {
				_ = tx.Rollback(ctx)
				errorCount.Add(1)
				return
			}

			if err := tx.Commit(ctx); err != nil {
				errorCount.Add(1)
				return
			}

			// Validate returned result version continuity
			if int64(len(result.Events)) != int64(eventsPerGoroutine) {
				t.Errorf("Goroutine %d: expected %d events, got %d", idx, eventsPerGoroutine, len(result.Events))
				errorCount.Add(1)
				return
			}
			if result.ToVersion()-result.FromVersion() != int64(eventsPerGoroutine) {
				t.Errorf("Goroutine %d: version range mismatch: from %d to %d (expected size %d)",
					idx, result.FromVersion(), result.ToVersion(), eventsPerGoroutine)
				errorCount.Add(1)
				return
			}

			successCount.Add(1)
		}(i)
	}

	close(startGate)
	wg.Wait()

	if errorCount.Load() != 0 {
		t.Fatalf("Expected 0 errors during concurrent Any() appends, got %d", errorCount.Load())
	}
	if successCount.Load() != int32(concurrency) {
		t.Fatalf("Expected all %d goroutines to succeed, got %d", concurrency, successCount.Load())
	}

	// Verify all events are present with contiguous stream versions 1..150
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin read tx: %v", err)
	}
	defer tx.Rollback(ctx)

	s, err := pgStore.ReadStream(ctx, tx, streamType, streamID, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read stream: %v", err)
	}

	expectedTotalEvents := concurrency * eventsPerGoroutine
	if len(s.Events) != expectedTotalEvents {
		t.Fatalf("Expected %d total events, got %d", expectedTotalEvents, len(s.Events))
	}
	if s.Version() != int64(expectedTotalEvents) {
		t.Fatalf("Expected final stream version %d, got %d", expectedTotalEvents, s.Version())
	}

	// Verify continuous stream versions with no gaps or duplicates
	seenVersions := make(map[int64]bool)
	for i, e := range s.Events {
		expectedVersion := int64(i + 1)
		if e.StreamVersion != expectedVersion {
			t.Errorf("Event %d: expected StreamVersion %d, got %d", i, expectedVersion, e.StreamVersion)
		}
		if seenVersions[e.StreamVersion] {
			t.Errorf("Duplicate StreamVersion detected: %d", e.StreamVersion)
		}
		seenVersions[e.StreamVersion] = true
	}
}

// TestConcurrent_Rollback_RestoresHead verifies that when a transaction rolls back,
// the stream_heads reservation is undone and doesn't block subsequent appends.
func TestConcurrent_Rollback_RestoresHead(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := uuid.New().String()
	streamType := "Document"

	event1 := store.Event{
		StreamType:   streamType,
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "DocumentCreated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	// TX 1: Append and rollback
	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.NoStream(), []store.Event{event1})
	if err != nil {
		t.Fatalf("Append in TX 1 failed: %v", err)
	}
	_ = tx1.Rollback(ctx)

	// TX 2: Append with NoStream() should now succeed because TX 1 rolled back
	event2 := store.Event{
		StreamType:   streamType,
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "DocumentCreated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	tx2, _ := db.BeginTx(ctx, nil)
	res, err := pgStore.Append(ctx, tx2, store.NoStream(), []store.Event{event2})
	if err != nil {
		t.Fatalf("Append with NoStream() after rollback failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit TX 2 failed: %v", err)
	}

	if res.ToVersion() != 1 {
		t.Fatalf("Expected stream version 1, got %d", res.ToVersion())
	}
}

// TestConcurrent_MixedCommitAndRollback verifies consistent stream versioning
// when concurrent writers commit and rollback concurrently.
func TestConcurrent_MixedCommitAndRollback(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := uuid.New().String()
	streamType := "Ledger"

	commitCount := 20
	rollbackCount := 20
	totalGoroutines := commitCount + rollbackCount
	eventsPerBatch := 2

	var wg sync.WaitGroup
	var successfulCommits atomic.Int32
	var rolledBackCount atomic.Int32

	startGate := make(chan struct{})

	for i := 0; i < totalGoroutines; i++ {
		wg.Add(1)
		shouldRollback := (i % 2 == 0) // alternate rollback and commit

		go func(idx int, rollback bool) {
			defer wg.Done()
			ctx := context.Background()

			var events []store.Event
			for j := 0; j < eventsPerBatch; j++ {
				events = append(events, store.Event{
					StreamType:   streamType,
					StreamID:     streamID,
					EventID:      uuid.New(),
					EventType:    "LedgerEntry",
					EventVersion: 1,
					Payload:      []byte(fmt.Sprintf(`{"entry": %d}`, idx)),
					CreatedAt:    time.Now(),
				})
			}

			<-startGate

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return
			}

			_, err = pgStore.Append(ctx, tx, store.Any(), events)
			if err != nil {
				_ = tx.Rollback(ctx)
				return
			}

			if rollback {
				_ = tx.Rollback(ctx)
				rolledBackCount.Add(1)
			} else {
				if err := tx.Commit(ctx); err == nil {
					successfulCommits.Add(1)
				}
			}
		}(i, shouldRollback)
	}

	close(startGate)
	wg.Wait()

	if successfulCommits.Load() != int32(commitCount) {
		t.Fatalf("Expected %d successful commits, got %d", commitCount, successfulCommits.Load())
	}

	// Verify committed events in stream
	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, nil)
	defer tx.Rollback(ctx)

	s, err := pgStore.ReadStream(ctx, tx, streamType, streamID, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read stream: %v", err)
	}

	expectedEvents := commitCount * eventsPerBatch
	if len(s.Events) != expectedEvents {
		t.Fatalf("Expected %d committed events, got %d", expectedEvents, len(s.Events))
	}
	if s.Version() != int64(expectedEvents) {
		t.Fatalf("Expected final version %d, got %d", expectedEvents, s.Version())
	}
}

// TestConcurrent_MultiStreamContention verifies that high contention across multiple
// independent streams does not cause cross-stream blocking or deadlocks.
func TestConcurrent_MultiStreamContention(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	numStreams := 10
	goroutinesPerStream := 10
	streamType := "Tenant"

	streamIDs := make([]string, numStreams)
	for i := 0; i < numStreams; i++ {
		streamIDs[i] = uuid.New().String()
	}

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errCount atomic.Int32

	startGate := make(chan struct{})

	for streamIdx := 0; streamIdx < numStreams; streamIdx++ {
		sID := streamIDs[streamIdx]
		for g := 0; g < goroutinesPerStream; g++ {
			wg.Add(1)
			go func(streamID string) {
				defer wg.Done()
				ctx := context.Background()

				event := store.Event{
					StreamType:   streamType,
					StreamID:     streamID,
					EventID:      uuid.New(),
					EventType:    "TenantActivity",
					EventVersion: 1,
					Payload:      []byte(`{}`),
					CreatedAt:    time.Now(),
				}

				<-startGate

				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					errCount.Add(1)
					return
				}

				_, err = pgStore.Append(ctx, tx, store.Any(), []store.Event{event})
				if err != nil {
					_ = tx.Rollback(ctx)
					errCount.Add(1)
					return
				}

				if err := tx.Commit(ctx); err != nil {
					errCount.Add(1)
					return
				}

				successCount.Add(1)
			}(sID)
		}
	}

	close(startGate)
	wg.Wait()

	totalExpected := numStreams * goroutinesPerStream
	if errCount.Load() != 0 {
		t.Fatalf("Expected 0 errors across multi-stream test, got %d", errCount.Load())
	}
	if successCount.Load() != int32(totalExpected) {
		t.Fatalf("Expected %d total successes, got %d", totalExpected, successCount.Load())
	}

	// Verify each stream has exactly goroutinesPerStream events
	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, nil)
	defer tx.Rollback(ctx)

	for _, sID := range streamIDs {
		s, err := pgStore.ReadStream(ctx, tx, streamType, sID, nil, nil)
		if err != nil {
			t.Fatalf("Failed to read stream %s: %v", sID, err)
		}
		if len(s.Events) != goroutinesPerStream {
			t.Errorf("Stream %s: expected %d events, got %d", sID, goroutinesPerStream, len(s.Events))
		}
		if s.Version() != int64(goroutinesPerStream) {
			t.Errorf("Stream %s: expected version %d, got %d", sID, goroutinesPerStream, s.Version())
		}
	}
}

// TestConcurrent_VariableBatchSizes verifies that concurrent appends with
// varying batch sizes correctly calculate FromVersion and ToVersion.
func TestConcurrent_VariableBatchSizes(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := uuid.New().String()
	streamType := "BatchStream"

	batchSizes := []int{1, 2, 3, 5, 8, 1, 4, 2, 6, 3}
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var totalEventsAppended atomic.Int64

	startGate := make(chan struct{})

	for i, size := range batchSizes {
		wg.Add(1)
		go func(idx, batchSize int) {
			defer wg.Done()
			ctx := context.Background()

			var events []store.Event
			for j := 0; j < batchSize; j++ {
				events = append(events, store.Event{
					StreamType:   streamType,
					StreamID:     streamID,
					EventID:      uuid.New(),
					EventType:    "VariableEvent",
					EventVersion: 1,
					Payload:      []byte(fmt.Sprintf(`{"batch": %d, "item": %d}`, idx, j)),
					CreatedAt:    time.Now(),
				})
			}

			<-startGate

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return
			}

			result, err := pgStore.Append(ctx, tx, store.Any(), events)
			if err != nil {
				_ = tx.Rollback(ctx)
				return
			}

			if err := tx.Commit(ctx); err != nil {
				return
			}

			if result.ToVersion()-result.FromVersion() != int64(batchSize) {
				t.Errorf("Batch %d: version range mismatch: from %d to %d (expected size %d)",
					idx, result.FromVersion(), result.ToVersion(), batchSize)
			}

			successCount.Add(1)
			totalEventsAppended.Add(int64(batchSize))
		}(i, size)
	}

	close(startGate)
	wg.Wait()

	if successCount.Load() != int32(len(batchSizes)) {
		t.Fatalf("Expected %d batch successes, got %d", len(batchSizes), successCount.Load())
	}

	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, nil)
	defer tx.Rollback(ctx)

	s, err := pgStore.ReadStream(ctx, tx, streamType, streamID, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read stream: %v", err)
	}

	if int64(len(s.Events)) != totalEventsAppended.Load() {
		t.Fatalf("Expected %d total events, got %d", totalEventsAppended.Load(), len(s.Events))
	}
	if s.Version() != totalEventsAppended.Load() {
		t.Fatalf("Expected stream version %d, got %d", totalEventsAppended.Load(), s.Version())
	}
}

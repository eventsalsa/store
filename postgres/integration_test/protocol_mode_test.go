//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/eventsalsa/store"
	"github.com/eventsalsa/store/postgres"
)

// TestProtocolMode_SimpleProtocolMode verifies end-to-end event append and read
// using pgx simple protocol mode (simulating pgBouncer in transaction pooling mode).
func TestProtocolMode_SimpleProtocolMode(t *testing.T) {
	config, err := pgxpool.ParseConfig(testDBConnStr)
	if err != nil {
		t.Fatalf("Failed to parse pool config: %v", err)
	}
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("Failed to connect with simple protocol mode: %v", err)
	}
	defer pool.Close()

	db := testDB{pool}
	setupTestTables(t, db)

	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := uuid.New().String()

	event := store.Event{
		StreamType:    "SimpleProtocolStream",
		StreamID:      streamID,
		EventID:       uuid.New(),
		EventType:     "TestSimpleProtocol",
		EventVersion:  1,
		Payload:       []byte(`{"simple_protocol": true}`),
		Metadata:      []byte(`{"client": "pgbouncer", "tags": ["prod", "v1"]}`),
		TraceID:       store.NullString{String: "trace-999", Valid: true},
		CorrelationID: store.NullString{String: "corr-888", Valid: true},
		CausationID:   store.NullString{Valid: false}, // NULL
		CreatedAt:     time.Now(),
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	appendResult, err := pgStore.Append(ctx, tx, store.NoStream(), []store.Event{event})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Append under simple protocol failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if appendResult.ToVersion() != 1 {
		t.Errorf("Expected ToVersion 1, got %d", appendResult.ToVersion())
	}

	// Read stream under simple protocol mode
	txRead, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx for read failed: %v", err)
	}
	defer txRead.Rollback(ctx)

	s, err := pgStore.ReadStream(ctx, txRead, "SimpleProtocolStream", streamID, nil, nil)
	if err != nil {
		t.Fatalf("ReadStream failed: %v", err)
	}

	if len(s.Events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(s.Events))
	}

	pe := s.Events[0]
	if pe.TraceID.String != "trace-999" || !pe.TraceID.Valid {
		t.Errorf("TraceID mismatch: %+v", pe.TraceID)
	}
	if pe.CorrelationID.String != "corr-888" || !pe.CorrelationID.Valid {
		t.Errorf("CorrelationID mismatch: %+v", pe.CorrelationID)
	}
	if pe.CausationID.Valid {
		t.Errorf("CausationID expected Valid=false, got %+v", pe.CausationID)
	}
}

// TestPayloadAndMetadata_BinaryAndComplexJSON verifies arbitrary binary bytes
// and complex JSON structures are faithfully preserved.
func TestPayloadAndMetadata_BinaryAndComplexJSON(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := uuid.New().String()

	// Binary payload containing null bytes, high bytes, and random non-UTF8 bytes
	binaryPayload := []byte{0x00, 0xFF, 0xFE, 0x01, 0x80, 0x7F, 0x00, 0xAA, 0x55}

	// Large 64KB payload
	largePayload := make([]byte, 65536)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	// Complex nested JSON metadata
	complexMetadata := []byte(`{"user":{"id":123,"roles":["admin","editor"]},"active":true,"rate":99.95}`)

	events := []store.Event{
		{
			StreamType:   "BinaryStream",
			StreamID:     streamID,
			EventID:      uuid.New(),
			EventType:    "BinaryData",
			EventVersion: 1,
			Payload:      binaryPayload,
			Metadata:     complexMetadata,
			CreatedAt:    time.Now(),
		},
		{
			StreamType:   "BinaryStream",
			StreamID:     streamID,
			EventID:      uuid.New(),
			EventType:    "LargeData",
			EventVersion: 1,
			Payload:      largePayload,
			Metadata:     nil, // nil metadata
			CreatedAt:    time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx, store.NoStream(), events)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	_ = tx.Commit(ctx)

	// Read and verify payloads
	txRead, _ := db.BeginTx(ctx, nil)
	defer txRead.Rollback(ctx)

	s, err := pgStore.ReadStream(ctx, txRead, "BinaryStream", streamID, nil, nil)
	if err != nil {
		t.Fatalf("ReadStream failed: %v", err)
	}

	if !bytes.Equal(s.Events[0].Payload, binaryPayload) {
		t.Errorf("Binary payload corrupted: got %v, want %v", s.Events[0].Payload, binaryPayload)
	}
	if !bytes.Equal(s.Events[1].Payload, largePayload) {
		t.Errorf("Large 64KB payload corrupted")
	}
	if !strings.Contains(string(s.Events[0].Metadata), `"roles"`) {
		t.Errorf("Metadata JSONB not preserved: %s", string(s.Events[0].Metadata))
	}
}

// TestStore_InputValidationAndBoundaryConditions verifies error handling
// for mismatched stream types, stream IDs, and empty slices.
func TestStore_InputValidationAndBoundaryConditions(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	tx, _ := db.BeginTx(ctx, nil)
	defer tx.Rollback(ctx)

	// 1. Empty events slice returns ErrNoEvents
	_, err := pgStore.Append(ctx, tx, store.Any(), []store.Event{})
	if err != store.ErrNoEvents {
		t.Errorf("Expected ErrNoEvents, got: %v", err)
	}

	// 2. StreamType mismatch
	mismatchedTypeEvents := []store.Event{
		{
			StreamType: "TypeA",
			StreamID:   "id-1",
			EventID:    uuid.New(),
			EventType:  "EventA",
			Payload:    []byte(`{}`),
			CreatedAt:  time.Now(),
		},
		{
			StreamType: "TypeB", // mismatch
			StreamID:   "id-1",
			EventID:    uuid.New(),
			EventType:  "EventB",
			Payload:    []byte(`{}`),
			CreatedAt:  time.Now(),
		},
	}
	_, err = pgStore.Append(ctx, tx, store.NoStream(), mismatchedTypeEvents)
	if err == nil || !strings.Contains(err.Error(), "stream type mismatch") {
		t.Errorf("Expected stream type mismatch error, got: %v", err)
	}

	// 3. StreamID mismatch
	mismatchedIDEvents := []store.Event{
		{
			StreamType: "TypeA",
			StreamID:   "id-1",
			EventID:    uuid.New(),
			EventType:  "EventA",
			Payload:    []byte(`{}`),
			CreatedAt:  time.Now(),
		},
		{
			StreamType: "TypeA",
			StreamID:   "id-2", // mismatch
			EventID:    uuid.New(),
			EventType:  "EventB",
			Payload:    []byte(`{}`),
			CreatedAt:  time.Now(),
		},
	}
	_, err = pgStore.Append(ctx, tx, store.NoStream(), mismatchedIDEvents)
	if err == nil || !strings.Contains(err.Error(), "stream ID mismatch") {
		t.Errorf("Expected stream ID mismatch error, got: %v", err)
	}

	// 4. ReadStream with inverted version bounds (fromVersion > toVersion)
	fromV := int64(10)
	toV := int64(5)
	emptyStream, err := pgStore.ReadStream(ctx, tx, "TypeA", "id-1", &fromV, &toV)
	if err != nil {
		t.Errorf("Inverted range read should not error, got: %v", err)
	}
	if !emptyStream.IsEmpty() {
		t.Errorf("Inverted range read expected empty stream, got %d events", len(emptyStream.Events))
	}

	// 5. ReadEvents with position beyond highest allocated
	eventsBeyond, err := pgStore.ReadEvents(ctx, tx, 999999, 10)
	if err != nil {
		t.Errorf("ReadEvents beyond range should not error, got: %v", err)
	}
	if len(eventsBeyond) != 0 {
		t.Errorf("Expected 0 events, got %d", len(eventsBeyond))
	}
}

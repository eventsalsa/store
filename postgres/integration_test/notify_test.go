//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/eventsalsa/store"
	"github.com/eventsalsa/store/postgres"
)

// TestNotify_AppendNotification verifies that when WithNotifyChannel is configured,
// committing an append operation issues a pg_notify payload containing the last global_position.
func TestNotify_AppendNotification(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	channelName := "eventsalsa_test_notify"

	// Dedicated listener connection
	listenerConn, err := db.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire listener connection: %v", err)
	}
	defer listenerConn.Release()

	_, err = listenerConn.Exec(ctx, "LISTEN "+channelName)
	if err != nil {
		t.Fatalf("Failed to LISTEN on channel: %v", err)
	}

	pgStore := postgres.NewStore(postgres.NewStoreConfig(
		postgres.WithNotifyChannel(channelName),
	))

	streamID := uuid.New().String()
	events := []store.Event{
		{
			StreamType:   "Order",
			StreamID:     streamID,
			EventID:      uuid.New(),
			EventType:    "OrderCreated",
			EventVersion: 1,
			Payload:      []byte(`{}`),
			CreatedAt:    time.Now(),
		},
		{
			StreamType:   "Order",
			StreamID:     streamID,
			EventID:      uuid.New(),
			EventType:    "OrderConfirmed",
			EventVersion: 1,
			Payload:      []byte(`{}`),
			CreatedAt:    time.Now(),
		},
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin tx: %v", err)
	}

	result, err := pgStore.Append(ctx, tx, store.NoStream(), events)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	lastPosition := result.GlobalPositions[len(result.GlobalPositions)-1]

	waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
	defer waitCancel()

	notification, err := listenerConn.Conn().WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("Failed waiting for notification: %v", err)
	}

	if notification.Channel != channelName {
		t.Errorf("Expected notification channel %s, got %s", channelName, notification.Channel)
	}

	expectedPayload := fmt.Sprintf("%d", lastPosition)
	if notification.Payload != expectedPayload {
		t.Errorf("Expected notification payload %s, got %s", expectedPayload, notification.Payload)
	}
}

// TestNotify_RollbackSuppressed verifies that when an append transaction is rolled back,
// no notification is received by the listener.
func TestNotify_RollbackSuppressed(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	channelName := "eventsalsa_test_rollback_notify"

	listenerConn, err := db.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire listener connection: %v", err)
	}
	defer listenerConn.Release()

	_, err = listenerConn.Exec(ctx, "LISTEN "+channelName)
	if err != nil {
		t.Fatalf("Failed to LISTEN on channel: %v", err)
	}

	pgStore := postgres.NewStore(postgres.NewStoreConfig(
		postgres.WithNotifyChannel(channelName),
	))

	streamID := uuid.New().String()
	event := store.Event{
		StreamType:   "Order",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "OrderCreated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin tx: %v", err)
	}

	_, err = pgStore.Append(ctx, tx, store.NoStream(), []store.Event{event})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Rollback
	_ = tx.Rollback(ctx)

	waitCtx, waitCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer waitCancel()

	notification, err := listenerConn.Conn().WaitForNotification(waitCtx)
	if err == nil {
		t.Fatalf("Expected no notification on rollback, but received: %+v", notification)
	}
}

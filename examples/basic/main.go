// Package main demonstrates basic usage of the eventsalsa event store.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/eventsalsa/store"
	"github.com/eventsalsa/store/postgres"
)

//go:generate go run ../../cmd/migrate-gen -output ../../migrations -filename init.sql

// UserCreated is a sample event payload.
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// UserEmailChanged is a sample event payload.
type UserEmailChanged struct {
	OldEmail string `json:"old_email"`
	NewEmail string `json:"new_email"`
}

func main() {
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=eventsalsa_example sslmode=disable"

	ctx := context.Background()

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create event store
	eventStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// --- Example 1: Append events to a new aggregate ---
	fmt.Println("--- Example 1: Create New Aggregate ---")
	userID := uuid.New().String()

	payload1, err := json.Marshal(UserCreated{
		Email: "alice@example.com",
		Name:  "Alice Smith",
	})
	if err != nil {
		log.Fatalf("Failed to marshal event: %v", err)
	}

	events := []store.Event{
		{
			AggregateType: "User",
			AggregateID:   userID,
			EventID:       uuid.New(),
			EventType:     "UserCreated",
			EventVersion:  1,
			Payload:       payload1,
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		_ = tx.Rollback(ctx)
	}()

	result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("Failed to commit: %v", err)
	}

	fmt.Printf("Events appended at positions: %v\n", result.GlobalPositions)
	fmt.Printf("Aggregate is now at version: %d\n", result.ToVersion())

	// --- Example 2: Append to existing aggregate with version check ---
	fmt.Println("\n--- Example 2: Append with Optimistic Concurrency ---")

	payload2, err := json.Marshal(UserEmailChanged{
		OldEmail: "alice@example.com",
		NewEmail: "alice.smith@example.com",
	})
	if err != nil {
		log.Fatalf("Failed to marshal event: %v", err)
	}

	events2 := []store.Event{
		{
			AggregateType: "User",
			AggregateID:   userID,
			EventID:       uuid.New(),
			EventType:     "UserEmailChanged",
			EventVersion:  1,
			Payload:       payload2,
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
	}

	tx2, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	defer func() {
		//nolint:errcheck
		_ = tx2.Rollback(ctx)
	}()

	result2, err := eventStore.Append(ctx, tx2, store.Exact(1), events2)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	if err := tx2.Commit(ctx); err != nil {
		log.Fatalf("Failed to commit: %v", err)
	}

	fmt.Printf("Events appended. Version: %d → %d\n", result.ToVersion(), result2.ToVersion())

	// --- Example 3: Read aggregate stream ---
	fmt.Println("\n--- Example 3: Read Aggregate Stream ---")

	tx3, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	defer func() {
		//nolint:errcheck
		_ = tx3.Rollback(ctx)
	}()

	stream, err := eventStore.ReadAggregateStream(ctx, tx3, "User", userID, nil, nil)
	if err != nil {
		log.Fatalf("Failed to read aggregate stream: %v", err)
	}

	if err := tx3.Commit(ctx); err != nil {
		log.Fatalf("Failed to commit: %v", err)
	}

	fmt.Printf("Stream: %d events, version %d\n", stream.Len(), stream.Version())
	for i, event := range stream.Events {
		fmt.Printf("  Event %d: %s (v%d) at position %d\n",
			i+1, event.EventType, event.AggregateVersion, event.GlobalPosition)
	}

	// --- Example 4: Read events sequentially ---
	fmt.Println("\n--- Example 4: Read Events Sequentially ---")

	tx4, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	defer func() {
		//nolint:errcheck
		_ = tx4.Rollback(ctx)
	}()

	allEvents, err := eventStore.ReadEvents(ctx, tx4, 0, 100)
	if err != nil {
		log.Fatalf("Failed to read events: %v", err)
	}

	if err := tx4.Commit(ctx); err != nil {
		log.Fatalf("Failed to commit: %v", err)
	}

	fmt.Printf("Read %d events from the global log\n", len(allEvents))
	for _, event := range allEvents {
		fmt.Printf("  Position %d: %s/%s.%s (v%d)\n",
			event.GlobalPosition, event.AggregateType, event.AggregateID[:8], event.EventType, event.AggregateVersion)
	}
}

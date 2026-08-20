// Package migrations provides SQL migration generation for event sourcing infrastructure.
package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Config configures migration generation.
type Config struct {
	// OutputFolder is the directory where the migration file will be written
	OutputFolder string

	// OutputFilename is the name of the migration file
	OutputFilename string

	// EventsTable is the name of the events table
	EventsTable string

	// StreamHeadsTable is the name of the stream version tracking table
	StreamHeadsTable string
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	timestamp := time.Now().Format("20060102150405")
	return Config{
		OutputFolder:     "migrations",
		OutputFilename:   fmt.Sprintf("%s_init_event_sourcing.sql", timestamp),
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
	}
}

// GeneratePostgres generates a PostgreSQL migration file.
func GeneratePostgres(config *Config) error {
	// Ensure output folder exists
	if err := os.MkdirAll(config.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generatePostgresSQL(config)

	outputPath := filepath.Join(config.OutputFolder, config.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generatePostgresSQL(config *Config) string {
	return fmt.Sprintf(`-- Event Sourcing Infrastructure Migration
-- Generated: %s

-- Events table stores all domain events in append-only fashion
CREATE TABLE IF NOT EXISTS %s (
    global_position BIGSERIAL PRIMARY KEY,
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    event_id UUID NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    event_version INT NOT NULL DEFAULT 1,
    payload BYTEA NOT NULL,
    trace_id TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Ensure version uniqueness per stream
    UNIQUE (stream_type, stream_id, stream_version)
);

-- Index for stream reads
CREATE INDEX IF NOT EXISTS idx_%s_stream 
    ON %s (stream_type, stream_id, stream_version);

-- Index for event type queries
CREATE INDEX IF NOT EXISTS idx_%s_event_type 
    ON %s (event_type, global_position);

-- Index for correlation tracking
CREATE INDEX IF NOT EXISTS idx_%s_correlation 
    ON %s (correlation_id) WHERE correlation_id IS NOT NULL;

-- Index for scoped sequential reads (stream_type + global_position)
CREATE INDEX IF NOT EXISTS idx_%s_stream_type_position 
    ON %s (stream_type, global_position);

-- Stream heads table tracks the current version of each stream
-- Provides O(1) version lookup for event append operations
-- Primary key (stream_type, stream_id) ensures one row per stream
CREATE TABLE IF NOT EXISTS %s (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (stream_type, stream_id)
);

-- Index for observability
CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s (updated_at);
`,
		time.Now().Format(time.RFC3339),
		config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.StreamHeadsTable,
		config.StreamHeadsTable, config.StreamHeadsTable,
	)
}

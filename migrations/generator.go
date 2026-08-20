// Package migrations provides SQL migration generation for event sourcing infrastructure.
package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// PartitionStrategy defines the partitioning approach for the events table.
type PartitionStrategy string

const (
	// PartitionStrategyNone disables partitioning (default: standard unpartitioned events table).
	PartitionStrategyNone PartitionStrategy = "none"

	// PartitionStrategyNative generates PostgreSQL declarative RANGE partitioning with pre-created partitions.
	PartitionStrategyNative PartitionStrategy = "native"

	// PartitionStrategyPartman generates pg_partman extension setup for dynamic partition management.
	PartitionStrategyPartman PartitionStrategy = "partman"
)

// PartmanMaintenance defines how pg_partman maintenance is automated.
type PartmanMaintenance string

const (
	// PartmanMaintenanceNone generates standard partman setup without cron/bgw (manual or external cron).
	PartmanMaintenanceNone PartmanMaintenance = "none"

	// PartmanMaintenanceBGW generates setup comments and postgresql.conf instructions for pg_partman_bgw.
	PartmanMaintenanceBGW PartmanMaintenance = "bgw"

	// PartmanMaintenancePgCron generates pg_cron extension creation and scheduled maintenance job.
	PartmanMaintenancePgCron PartmanMaintenance = "pg_cron"
)

// PartitionConfig defines partitioning options for the events table.
type PartitionConfig struct {
	// Strategy determines how partitioning is handled (none, native, partman).
	Strategy PartitionStrategy

	// PartmanSchema is the schema where the pg_partman extension is installed. Defaults to "partman".
	PartmanSchema string

	// PartmanMaintenance specifies the automated maintenance mechanism for pg_partman (none, bgw, pg_cron).
	PartmanMaintenance PartmanMaintenance

	// PartitionSize is the number of global_position sequence values per partition (e.g. 10_000_000).
	// Defaults to 10,000,000 when partitioning is enabled.
	PartitionSize int64

	// InitialPartitions is the number of initial partitions to generate (for native or partman premake).
	// Defaults to 4.
	InitialPartitions int
}

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

	// Partitioning configures optional table partitioning for the events table
	Partitioning PartitionConfig
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	timestamp := time.Now().Format("20060102150405")
	return Config{
		OutputFolder:     "migrations",
		OutputFilename:   fmt.Sprintf("%s_init_event_sourcing.sql", timestamp),
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
		Partitioning: PartitionConfig{
			Strategy:           PartitionStrategyNone,
			PartitionSize:      10000000,
			InitialPartitions:  4,
			PartmanSchema:      "partman",
			PartmanMaintenance: PartmanMaintenanceNone,
		},
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
	switch config.Partitioning.Strategy {
	case PartitionStrategyNative:
		return generateNativePartitionedSQL(config)
	case PartitionStrategyPartman:
		return generatePartmanPartitionedSQL(config)
	default:
		return generateUnpartitionedSQL(config)
	}
}

func generateUnpartitionedSQL(config *Config) string {
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

func generateNativePartitionedSQL(config *Config) string {
	partitionSize := config.Partitioning.PartitionSize
	if partitionSize <= 0 {
		partitionSize = 10000000
	}

	initialPartitions := config.Partitioning.InitialPartitions
	if initialPartitions <= 0 {
		initialPartitions = 4
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, `-- Event Sourcing Infrastructure Migration (Native RANGE Partitioned)
-- Generated: %s

-- Sequence for global_position across partitions
CREATE SEQUENCE IF NOT EXISTS %s_global_position_seq;

-- Partitioned events table by global_position RANGE
CREATE TABLE IF NOT EXISTS %s (
    global_position BIGINT NOT NULL DEFAULT nextval('%s_global_position_seq'),
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_version INT NOT NULL DEFAULT 1,
    payload BYTEA NOT NULL,
    trace_id TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (global_position)
) PARTITION BY RANGE (global_position);
`,
		time.Now().Format(time.RFC3339),
		config.EventsTable,
		config.EventsTable,
		config.EventsTable,
	)

	sb.WriteString("\n-- Pre-allocated initial partitions\n")
	for p := 0; p < initialPartitions; p++ {
		fromPos := int64(p)*partitionSize + 1
		toPos := int64(p+1)*partitionSize + 1
		partitionName := fmt.Sprintf("%s_p%010d_p%010d", config.EventsTable, fromPos, toPos-1)

		fmt.Fprintf(&sb, "CREATE TABLE IF NOT EXISTS %s PARTITION OF %s\n    FOR VALUES FROM (%d) TO (%d);\n\n",
			partitionName, config.EventsTable, fromPos, toPos)
	}

	fmt.Fprintf(&sb, `-- Indexes on parent table (automatically applied to child partitions)
CREATE INDEX IF NOT EXISTS idx_%s_stream 
    ON %s (stream_type, stream_id, stream_version);

CREATE INDEX IF NOT EXISTS idx_%s_event_type 
    ON %s (event_type, global_position);

CREATE INDEX IF NOT EXISTS idx_%s_correlation 
    ON %s (correlation_id) WHERE correlation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_%s_stream_type_position 
    ON %s (stream_type, global_position);

-- Stream heads table tracks the current version of each stream
CREATE TABLE IF NOT EXISTS %s (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (stream_type, stream_id)
);

CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s (updated_at);
`,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.StreamHeadsTable,
		config.StreamHeadsTable, config.StreamHeadsTable,
	)

	return sb.String()
}

func generatePartmanPartitionedSQL(config *Config) string {
	partitionSize := config.Partitioning.PartitionSize
	if partitionSize <= 0 {
		partitionSize = 10000000
	}

	premake := config.Partitioning.InitialPartitions
	if premake <= 0 {
		premake = 4
	}

	partmanSchema := config.Partitioning.PartmanSchema
	if partmanSchema == "" {
		partmanSchema = "partman"
	}

	parentTable := config.EventsTable
	if !strings.Contains(parentTable, ".") {
		parentTable = "public." + parentTable
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, `-- Event Sourcing Infrastructure Migration (pg_partman Managed)
-- Generated: %s

-- Ensure pg_partman schema and extension exist
CREATE SCHEMA IF NOT EXISTS %s;
CREATE EXTENSION IF NOT EXISTS pg_partman WITH SCHEMA %s;

-- Sequence for global_position across partitions
CREATE SEQUENCE IF NOT EXISTS %s_global_position_seq;

-- Partitioned events table by global_position RANGE
CREATE TABLE IF NOT EXISTS %s (
    global_position BIGINT NOT NULL DEFAULT nextval('%s_global_position_seq'),
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_version INT NOT NULL DEFAULT 1,
    payload BYTEA NOT NULL,
    trace_id TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (global_position)
) PARTITION BY RANGE (global_position);
`,
		time.Now().Format(time.RFC3339),
		partmanSchema,
		partmanSchema,
		config.EventsTable,
		config.EventsTable,
		config.EventsTable,
	)

	fmt.Fprintf(&sb, `
-- Indexes on parent table (automatically applied to child partitions)
CREATE INDEX IF NOT EXISTS idx_%s_stream 
    ON %s (stream_type, stream_id, stream_version);

CREATE INDEX IF NOT EXISTS idx_%s_event_type 
    ON %s (event_type, global_position);

CREATE INDEX IF NOT EXISTS idx_%s_correlation 
    ON %s (correlation_id) WHERE correlation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_%s_stream_type_position 
    ON %s (stream_type, global_position);

-- Stream heads table tracks the current version of each stream
CREATE TABLE IF NOT EXISTS %s (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (stream_type, stream_id)
);

CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s (updated_at);

-- Register partitioned table with pg_partman
SELECT %s.create_parent(
    p_parent_table => '%s',
    p_control => 'global_position',
    p_type => 'native',
    p_interval => '%d',
    p_premake => %d
);
`,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.StreamHeadsTable,
		config.StreamHeadsTable, config.StreamHeadsTable,
		partmanSchema,
		parentTable,
		partitionSize,
		premake,
	)

	switch config.Partitioning.PartmanMaintenance {
	case PartmanMaintenancePgCron:
		fmt.Fprintf(&sb, `
-- Automated maintenance with pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;
SELECT cron.schedule('partman-maintenance-%s', '0 * * * *', $$CALL %s.run_maintenance_proc()$$);
`, config.EventsTable, partmanSchema)

	case PartmanMaintenanceBGW:
		sb.WriteString(`
-- pg_partman Background Worker (BGW) Configuration:
-- To enable automatic maintenance via Postgres Background Worker, configure postgresql.conf:
--   shared_preload_libraries = 'pg_partman_bgw'
--   pg_partman_bgw.interval = 3600
--   pg_partman_bgw.role = 'postgres'
--   pg_partman_bgw.dbname = '<your_database_name>'
`)

	default:
		fmt.Fprintf(&sb, `
-- pg_partman Manual / External Maintenance:
-- Run or schedule the following maintenance procedure periodically:
--   CALL %s.run_maintenance_proc();
`, partmanSchema)
	}

	return sb.String()
}

package migrations

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratePostgres(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "test_migration.sql",
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	// Verify file was created
	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify essential components are present
	requiredStrings := []string{
		"CREATE TABLE IF NOT EXISTS events",
		"global_position BIGSERIAL PRIMARY KEY",
		"stream_type TEXT NOT NULL",
		"stream_id TEXT NOT NULL",
		"stream_version BIGINT NOT NULL",
		"event_id UUID NOT NULL UNIQUE",
		"event_type TEXT NOT NULL",
		"event_version INT NOT NULL DEFAULT 1",
		"payload BYTEA NOT NULL",
		"trace_id TEXT",
		"correlation_id TEXT",
		"causation_id TEXT",
		"metadata JSONB",
		"created_at TIMESTAMPTZ NOT NULL",
		"CREATE TABLE IF NOT EXISTS stream_heads",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing required string: %s", required)
		}
	}

	// Verify indexes are created
	requiredIndexes := []string{
		"idx_events_stream",
		"idx_events_event_type",
		"idx_events_correlation",
		"idx_events_stream_type_position",
	}

	for _, idx := range requiredIndexes {
		if !strings.Contains(sql, idx) {
			t.Errorf("Generated SQL missing index: %s", idx)
		}
	}

	// Verify bounded_context is NOT present (removed)
	if strings.Contains(sql, "bounded_context") {
		t.Error("Generated SQL should not contain bounded_context (removed)")
	}

	// Verify consumer tables are NOT present (removed)
	if strings.Contains(sql, "consumer_segments") {
		t.Error("Generated SQL should not contain consumer_segments (removed)")
	}
	if strings.Contains(sql, "consumer_workers") {
		t.Error("Generated SQL should not contain consumer_workers (removed)")
	}

	// Verify correct unique constraint on events (no bounded_context)
	if !strings.Contains(sql, "UNIQUE (stream_type, stream_id, stream_version)") {
		t.Error("Generated SQL missing correct unique constraint on events")
	}

	// Verify correct primary key on stream_heads (no bounded_context)
	if !strings.Contains(sql, "PRIMARY KEY (stream_type, stream_id)") {
		t.Error("Generated SQL missing correct primary key on stream_heads")
	}
}

func TestGeneratePostgres_CustomTableNames(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "custom_migration.sql",
		EventsTable:      "custom_events",
		StreamHeadsTable: "custom_stream_heads",
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify custom table names are used
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_events") {
		t.Error("Custom events table name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_stream_heads") {
		t.Error("Custom stream_heads table name not used")
	}
}

func TestGeneratePostgres_NativePartitioning(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "native_partitioned.sql",
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
		Partitioning: PartitionConfig{
			Strategy:          PartitionStrategyNative,
			PartitionSize:     1000,
			InitialPartitions: 3,
			EventIDsTable:     "event_ids",
		},
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Check sequence creation
	if !strings.Contains(sql, "CREATE SEQUENCE IF NOT EXISTS events_global_position_seq") {
		t.Error("Missing events sequence creation")
	}

	// Check partitioned table definition
	if !strings.Contains(sql, "PARTITION BY RANGE (global_position)") {
		t.Error("Missing PARTITION BY RANGE (global_position)")
	}

	// Check companion table
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS event_ids") {
		t.Error("Missing event_ids companion table")
	}

	// Check initial partitions
	expectedPartitions := []string{
		"CREATE TABLE IF NOT EXISTS events_p0000000001_p0000001000 PARTITION OF events\n    FOR VALUES FROM (1) TO (1001);",
		"CREATE TABLE IF NOT EXISTS events_p0000001001_p0000002000 PARTITION OF events\n    FOR VALUES FROM (1001) TO (2001);",
		"CREATE TABLE IF NOT EXISTS events_p0000002001_p0000003000 PARTITION OF events\n    FOR VALUES FROM (2001) TO (3001);",
	}

	for _, p := range expectedPartitions {
		if !strings.Contains(sql, p) {
			t.Errorf("Missing expected partition in SQL:\n%s", p)
		}
	}

	// Check stream_heads
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS stream_heads") {
		t.Error("Missing stream_heads table in partitioned SQL")
	}
}

func TestGeneratePostgres_Partman_PgCron(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "partman_cron.sql",
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
		Partitioning: PartitionConfig{
			Strategy:           PartitionStrategyPartman,
			PartitionSize:      5000000,
			InitialPartitions:  6,
			PartmanSchema:      "partman",
			PartmanMaintenance: PartmanMaintenancePgCron,
			EventIDsTable:      "event_ids",
		},
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify partman extension and schema
	if !strings.Contains(sql, "CREATE EXTENSION IF NOT EXISTS pg_partman WITH SCHEMA partman;") {
		t.Error("Missing pg_partman extension creation")
	}

	// Verify partman create_parent call
	if !strings.Contains(sql, "SELECT partman.create_parent(") {
		t.Error("Missing partman.create_parent call")
	}
	if !strings.Contains(sql, "p_interval => '5000000'") {
		t.Error("Missing p_interval parameter in partman.create_parent")
	}
	if !strings.Contains(sql, "p_premake => 6") {
		t.Error("Missing p_premake parameter in partman.create_parent")
	}

	// Verify pg_cron configuration
	if !strings.Contains(sql, "CREATE EXTENSION IF NOT EXISTS pg_cron;") {
		t.Error("Missing pg_cron extension creation")
	}
	if !strings.Contains(sql, "SELECT cron.schedule('partman-maintenance-events', '0 * * * *', $$CALL partman.run_maintenance_proc()$$);") {
		t.Error("Missing cron.schedule call for partman maintenance")
	}
}

func TestGeneratePostgres_Partman_BGW(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "partman_bgw.sql",
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
		Partitioning: PartitionConfig{
			Strategy:           PartitionStrategyPartman,
			PartitionSize:      10000000,
			InitialPartitions:  4,
			PartmanSchema:      "partman",
			PartmanMaintenance: PartmanMaintenanceBGW,
			EventIDsTable:      "event_ids",
		},
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	if !strings.Contains(sql, "pg_partman Background Worker (BGW) Configuration:") {
		t.Error("Missing BGW configuration comments")
	}
	if !strings.Contains(sql, "shared_preload_libraries = 'pg_partman_bgw'") {
		t.Error("Missing shared_preload_libraries instruction in BGW comment")
	}
}

func TestGeneratePostgres_Partman_Manual(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "partman_manual.sql",
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
		Partitioning: PartitionConfig{
			Strategy:           PartitionStrategyPartman,
			PartitionSize:      10000000,
			InitialPartitions:  4,
			PartmanSchema:      "partman",
			PartmanMaintenance: PartmanMaintenanceNone,
			EventIDsTable:      "event_ids",
		},
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	if !strings.Contains(sql, "CALL partman.run_maintenance_proc();") {
		t.Error("Missing manual CALL partman.run_maintenance_proc() instruction")
	}
}

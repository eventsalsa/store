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

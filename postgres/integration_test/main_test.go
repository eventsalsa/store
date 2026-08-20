//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/eventsalsa/store/migrations"
)

var testDBConnStr string

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("eventsalsa_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatalf("failed to start postgres container: %v", err)
	}

	testDBConnStr, err = pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgContainer.Terminate(context.Background())
		log.Fatalf("failed to get connection string: %v", err)
	}

	code := m.Run()

	termCtx, termCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer termCancel()
	if err := pgContainer.Terminate(termCtx); err != nil {
		log.Printf("failed to terminate container: %v", err)
	}

	os.Exit(code)
}

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

	config, err := pgxpool.ParseConfig(testDBConnStr)
	if err != nil {
		t.Fatalf("Failed to parse connection string: %v", err)
	}

	if os.Getenv("PGX_TEST_SIMPLE_PROTOCOL") == "true" {
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}
	switch os.Getenv("PGX_TEST_QUERY_EXEC_MODE") {
	case "cache_statement":
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	case "cache_describe":
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	case "describe_exec":
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	case "exec":
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	case "simple_protocol":
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
		DROP TABLE IF EXISTS stream_heads CASCADE;
		DROP TABLE IF EXISTS events CASCADE;
	`)
	if err != nil {
		t.Fatalf("Failed to drop tables: %v", err)
	}

	// Generate and execute migration
	tmpDir := t.TempDir()
	config := &migrations.Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "test.sql",
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
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

// Command migrate-gen generates SQL migration files for event sourcing.
//
// Usage:
//
//	go run github.com/eventsalsa/store/cmd/migrate-gen -output migrations -filename init.sql
//
// Or with go generate:
//
//	//go:generate go run github.com/eventsalsa/store/cmd/migrate-gen -output migrations
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/eventsalsa/store/migrations"
)

func main() {
	var (
		outputFolder       = flag.String("output", "migrations", "Output folder for migration file")
		outputFilename     = flag.String("filename", "", "Output filename (default: timestamp-based)")
		eventsTable        = flag.String("events-table", "events", "Name of events table")
		streamHeadsTable   = flag.String("stream-heads-table", "stream_heads", "Name of stream heads table")
		partitionStrategy  = flag.String("partition-strategy", "none", "Partition strategy: none, native, partman")
		partitionSize      = flag.Int64("partition-size", 10000000, "Number of global_position IDs per partition")
		initialPartitions  = flag.Int("initial-partitions", 4, "Number of initial partitions to generate or premake")
		partmanSchema      = flag.String("partman-schema", "partman", "Schema where pg_partman is installed")
		partmanMaintenance = flag.String("partman-maintenance", "none", "pg_partman maintenance mode: none, bgw, pg_cron")
		eventIDsTable      = flag.String("event-ids-table", "event_ids", "Name of companion table for global event_id uniqueness")
	)

	flag.Parse()

	config := migrations.DefaultConfig()
	config.OutputFolder = *outputFolder
	config.EventsTable = *eventsTable
	config.StreamHeadsTable = *streamHeadsTable
	config.Partitioning.Strategy = migrations.PartitionStrategy(*partitionStrategy)
	config.Partitioning.PartitionSize = *partitionSize
	config.Partitioning.InitialPartitions = *initialPartitions
	config.Partitioning.PartmanSchema = *partmanSchema
	config.Partitioning.PartmanMaintenance = migrations.PartmanMaintenance(*partmanMaintenance)
	config.Partitioning.EventIDsTable = *eventIDsTable

	if *outputFilename != "" {
		config.OutputFilename = *outputFilename
	}

	err := migrations.GeneratePostgres(&config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating migration: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generated PostgreSQL migration: %s/%s\n", config.OutputFolder, config.OutputFilename)
}

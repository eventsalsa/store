package store

import "context"

// Logger provides a minimal interface for observability and debugging.
// It is designed to be optional and non-blocking, with zero overhead when disabled.
// Users can implement this interface to integrate their preferred logging library.
type Logger interface {
	// Debug logs debug-level information for detailed troubleshooting.
	// Typically used for verbose operational details.
	Debug(ctx context.Context, msg string, keyvals ...interface{})

	// Info logs informational messages about normal operations.
	// Used to track significant events during normal execution.
	Info(ctx context.Context, msg string, keyvals ...interface{})

	// Error logs error-level information about failures.
	// Used to track errors that require attention.
	Error(ctx context.Context, msg string, keyvals ...interface{})
}

// NoOpLogger is a logger that does nothing.
// It can be used as a default when no logging is desired.
type NoOpLogger struct{}

// Debug implements Logger.
func (NoOpLogger) Debug(_ context.Context, _ string, _ ...interface{}) {}

// Info implements Logger.
func (NoOpLogger) Info(_ context.Context, _ string, _ ...interface{}) {}

// Error implements Logger.
func (NoOpLogger) Error(_ context.Context, _ string, _ ...interface{}) {}

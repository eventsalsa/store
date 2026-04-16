package store

import "fmt"

// ExpectedVersion represents the expected aggregate version for optimistic concurrency control.
// It is used in the Append operation to declare expectations about the current state of an aggregate.
type ExpectedVersion struct {
	value int64
}

const (
	// expectedVersionAny indicates no version check should be performed
	expectedVersionAny = -1
	// expectedVersionNoStream indicates the aggregate must not exist
	expectedVersionNoStream = 0
)

// Any returns an ExpectedVersion that skips version validation.
// Use this when you don't need optimistic concurrency control.
func Any() ExpectedVersion {
	return ExpectedVersion{value: expectedVersionAny}
}

// NoStream returns an ExpectedVersion that enforces the aggregate must not exist.
// Use this when creating a new aggregate to ensure it doesn't already exist.
// This is useful for enforcing uniqueness constraints via reservation aggregates.
func NoStream() ExpectedVersion {
	return ExpectedVersion{value: expectedVersionNoStream}
}

// Exact returns an ExpectedVersion that enforces the aggregate must be at exactly the specified version.
// Use this for normal command handling with optimistic concurrency control.
// The version must be non-negative (>= 0). Note that Exact(0) is equivalent to NoStream().
func Exact(version int64) ExpectedVersion {
	if version < 0 {
		panic(fmt.Sprintf("exact version must be non-negative, got %d", version))
	}
	return ExpectedVersion{value: version}
}

// IsAny returns true if this is an "Any" expected version (no version check).
func (ev ExpectedVersion) IsAny() bool {
	return ev.value == expectedVersionAny
}

// IsNoStream returns true if this is a "NoStream" expected version (aggregate must not exist).
func (ev ExpectedVersion) IsNoStream() bool {
	return ev.value == expectedVersionNoStream
}

// IsExact returns true if this is an "Exact" expected version (aggregate must be at specific version).
func (ev ExpectedVersion) IsExact() bool {
	return ev.value > 0
}

// Value returns the exact version number if this is an Exact expected version.
// Returns 0 for Any and NoStream.
func (ev ExpectedVersion) Value() int64 {
	if ev.value > 0 {
		return ev.value
	}
	return 0
}

// String returns a string representation of the ExpectedVersion.
func (ev ExpectedVersion) String() string {
	if ev.IsAny() {
		return "Any"
	}
	if ev.IsNoStream() {
		return "NoStream"
	}
	return fmt.Sprintf("Exact(%d)", ev.value)
}

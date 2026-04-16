package store

import (
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/google/uuid"
)

// NullString represents a string that may be null.
// It implements database/sql Scanner and Valuer interfaces for SQL interop,
// but avoids direct dependency on sql.NullString in public types.
type NullString struct {
	String string
	Valid  bool // Valid is true if String is not NULL
}

// Scan implements the sql.Scanner interface.
func (ns *NullString) Scan(value interface{}) error {
	if value == nil {
		ns.String, ns.Valid = "", false
		return nil
	}
	var s sql.NullString
	if err := s.Scan(value); err != nil {
		return err
	}
	ns.String, ns.Valid = s.String, s.Valid
	return nil
}

// Value implements the driver.Valuer interface.
func (ns NullString) Value() (driver.Value, error) {
	if !ns.Valid || ns.String == "" {
		return nil, nil
	}
	return ns.String, nil
}

// Event represents an immutable domain event before persistence.
// Events are value objects without identity until persisted.
// AggregateVersion and GlobalPosition are assigned by the store during Append.
type Event struct {
	CreatedAt     time.Time
	AggregateType string
	EventType     string
	AggregateID   string
	Payload       []byte
	Metadata      []byte
	CausationID   NullString
	CorrelationID NullString
	TraceID       NullString
	EventVersion  int
	EventID       uuid.UUID
}

// PersistedEvent represents an event that has been stored.
// It includes the GlobalPosition and AggregateVersion assigned by the event store.
type PersistedEvent struct {
	CreatedAt        time.Time
	AggregateType    string
	EventType        string
	AggregateID      string
	CausationID      NullString
	Metadata         []byte
	Payload          []byte
	CorrelationID    NullString
	TraceID          NullString
	GlobalPosition   int64
	AggregateVersion int64
	EventVersion     int
	EventID          uuid.UUID
}

// Stream represents the full historical event stream for a single aggregate.
// It is immutable after creation and is returned from read operations.
// Stream must never be returned from Append operations.
type Stream struct {
	AggregateType string
	AggregateID   string
	Events        []PersistedEvent
}

// Version returns the current version of the aggregate.
// If the stream is empty (no events), version is 0.
// Otherwise, version is the AggregateVersion of the last event in the stream.
func (s Stream) Version() int64 {
	if len(s.Events) == 0 {
		return 0
	}
	return s.Events[len(s.Events)-1].AggregateVersion
}

// IsEmpty returns true if the stream contains no events.
func (s Stream) IsEmpty() bool {
	return len(s.Events) == 0
}

// Len returns the number of events in the stream.
func (s Stream) Len() int {
	return len(s.Events)
}

// AppendResult represents the outcome of an Append operation.
// It contains only the events that were just committed, not the full history.
// AppendResult must never imply full history - use Stream for that purpose.
type AppendResult struct {
	Events          []PersistedEvent
	GlobalPositions []int64
}

// FromVersion returns the aggregate version before the append.
// If no events were appended, returns 0.
// Otherwise, returns the version immediately before the first appended event.
func (r AppendResult) FromVersion() int64 {
	if len(r.Events) == 0 {
		return 0
	}
	return r.Events[0].AggregateVersion - 1
}

// ToVersion returns the aggregate version after the append.
// If no events were appended, returns 0.
// Otherwise, returns the AggregateVersion of the last appended event.
func (r AppendResult) ToVersion() int64 {
	if len(r.Events) == 0 {
		return 0
	}
	return r.Events[len(r.Events)-1].AggregateVersion
}

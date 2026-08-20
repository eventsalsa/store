package store

import (
	"database/sql/driver"
	"fmt"
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
	switch v := value.(type) {
	case string:
		ns.String, ns.Valid = v, true
		return nil
	case []byte:
		ns.String, ns.Valid = string(v), true
		return nil
	default:
		return fmt.Errorf("cannot scan %T into NullString", value)
	}
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
// StreamVersion and GlobalPosition are assigned by the store during Append.
type Event struct {
	CreatedAt     time.Time
	StreamType    string
	EventType     string
	StreamID      string
	Payload       []byte
	Metadata      []byte
	CausationID   NullString
	CorrelationID NullString
	TraceID       NullString
	EventVersion  int
	EventID       uuid.UUID
}

// PersistedEvent represents an event that has been stored.
// It includes the GlobalPosition and StreamVersion assigned by the event store.
type PersistedEvent struct {
	CreatedAt      time.Time
	StreamType     string
	EventType      string
	StreamID       string
	CausationID    NullString
	Metadata       []byte
	Payload        []byte
	CorrelationID  NullString
	TraceID        NullString
	GlobalPosition int64
	StreamVersion  int64
	EventVersion   int
	EventID        uuid.UUID
}

// Stream represents the full historical event stream for a single stream.
// It is immutable after creation and is returned from read operations.
// Stream must never be returned from Append operations.
type Stream struct {
	StreamType string
	StreamID   string
	Events     []PersistedEvent
}

// Version returns the current version of the stream.
// If the stream is empty (no events), version is 0.
// Otherwise, version is the StreamVersion of the last event in the stream.
func (s Stream) Version() int64 {
	if len(s.Events) == 0 {
		return 0
	}
	return s.Events[len(s.Events)-1].StreamVersion
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

// FromVersion returns the stream version before the append.
// If no events were appended, returns 0.
// Otherwise, returns the version immediately before the first appended event.
func (r AppendResult) FromVersion() int64 {
	if len(r.Events) == 0 {
		return 0
	}
	return r.Events[0].StreamVersion - 1
}

// ToVersion returns the stream version after the append.
// If no events were appended, returns 0.
// Otherwise, returns the StreamVersion of the last appended event.
func (r AppendResult) ToVersion() int64 {
	if len(r.Events) == 0 {
		return 0
	}
	return r.Events[len(r.Events)-1].StreamVersion
}

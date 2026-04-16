package store

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStream_Version(t *testing.T) {
	tests := []struct {
		name   string
		stream Stream
		want   int64
	}{
		{
			name: "empty stream returns version 0",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events:        []PersistedEvent{},
			},
			want: 0,
		},
		{
			name: "stream with one event returns that event's version",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events: []PersistedEvent{
					{AggregateVersion: 1},
				},
			},
			want: 1,
		},
		{
			name: "stream with multiple events returns last event's version",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events: []PersistedEvent{
					{AggregateVersion: 1},
					{AggregateVersion: 2},
					{AggregateVersion: 3},
				},
			},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stream.Version(); got != tt.want {
				t.Errorf("Stream.Version() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStream_IsEmpty(t *testing.T) {
	tests := []struct {
		name   string
		stream Stream
		want   bool
	}{
		{
			name: "empty stream returns true",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events:        []PersistedEvent{},
			},
			want: true,
		},
		{
			name: "nil events slice returns true",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events:        nil,
			},
			want: true,
		},
		{
			name: "stream with events returns false",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events: []PersistedEvent{
					{AggregateVersion: 1},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stream.IsEmpty(); got != tt.want {
				t.Errorf("Stream.IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStream_Len(t *testing.T) {
	tests := []struct {
		name   string
		stream Stream
		want   int
	}{
		{
			name: "empty stream returns 0",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events:        []PersistedEvent{},
			},
			want: 0,
		},
		{
			name: "nil events slice returns 0",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events:        nil,
			},
			want: 0,
		},
		{
			name: "stream with one event returns 1",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events: []PersistedEvent{
					{AggregateVersion: 1},
				},
			},
			want: 1,
		},
		{
			name: "stream with multiple events returns correct count",
			stream: Stream{
				AggregateType: "User",
				AggregateID:   "123",
				Events: []PersistedEvent{
					{AggregateVersion: 1},
					{AggregateVersion: 2},
					{AggregateVersion: 3},
				},
			},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stream.Len(); got != tt.want {
				t.Errorf("Stream.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAppendResult_FromVersion(t *testing.T) {
	tests := []struct {
		name   string
		result AppendResult
		want   int64
	}{
		{
			name: "empty result returns 0",
			result: AppendResult{
				Events:          []PersistedEvent{},
				GlobalPositions: []int64{},
			},
			want: 0,
		},
		{
			name: "single event at version 1 returns 0 (no previous version)",
			result: AppendResult{
				Events: []PersistedEvent{
					{AggregateVersion: 1},
				},
				GlobalPositions: []int64{1},
			},
			want: 0,
		},
		{
			name: "single event at version 5 returns 4",
			result: AppendResult{
				Events: []PersistedEvent{
					{AggregateVersion: 5},
				},
				GlobalPositions: []int64{10},
			},
			want: 4,
		},
		{
			name: "multiple events starting at version 3 returns 2",
			result: AppendResult{
				Events: []PersistedEvent{
					{AggregateVersion: 3},
					{AggregateVersion: 4},
					{AggregateVersion: 5},
				},
				GlobalPositions: []int64{10, 11, 12},
			},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.result.FromVersion(); got != tt.want {
				t.Errorf("AppendResult.FromVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAppendResult_ToVersion(t *testing.T) {
	tests := []struct {
		name   string
		result AppendResult
		want   int64
	}{
		{
			name: "empty result returns 0",
			result: AppendResult{
				Events:          []PersistedEvent{},
				GlobalPositions: []int64{},
			},
			want: 0,
		},
		{
			name: "single event returns that event's version",
			result: AppendResult{
				Events: []PersistedEvent{
					{AggregateVersion: 1},
				},
				GlobalPositions: []int64{1},
			},
			want: 1,
		},
		{
			name: "multiple events returns last event's version",
			result: AppendResult{
				Events: []PersistedEvent{
					{AggregateVersion: 3},
					{AggregateVersion: 4},
					{AggregateVersion: 5},
				},
				GlobalPositions: []int64{10, 11, 12},
			},
			want: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.result.ToVersion(); got != tt.want {
				t.Errorf("AppendResult.ToVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAppendResult_FullWorkflow(t *testing.T) {
	aggregateID := uuid.New().String()

	// First append - creating new aggregate
	result1 := AppendResult{
		Events: []PersistedEvent{
			{
				AggregateType:    "User",
				AggregateID:      aggregateID,
				AggregateVersion: 1,
				EventType:        "UserCreated",
				GlobalPosition:   100,
				CreatedAt:        time.Now(),
			},
		},
		GlobalPositions: []int64{100},
	}

	if result1.FromVersion() != 0 {
		t.Errorf("First append FromVersion() = %v, want 0", result1.FromVersion())
	}
	if result1.ToVersion() != 1 {
		t.Errorf("First append ToVersion() = %v, want 1", result1.ToVersion())
	}

	// Second append - updating existing aggregate
	result2 := AppendResult{
		Events: []PersistedEvent{
			{
				AggregateType:    "User",
				AggregateID:      aggregateID,
				AggregateVersion: 2,
				EventType:        "UserUpdated",
				GlobalPosition:   101,
				CreatedAt:        time.Now(),
			},
			{
				AggregateType:    "User",
				AggregateID:      aggregateID,
				AggregateVersion: 3,
				EventType:        "UserActivated",
				GlobalPosition:   102,
				CreatedAt:        time.Now(),
			},
		},
		GlobalPositions: []int64{101, 102},
	}

	if result2.FromVersion() != 1 {
		t.Errorf("Second append FromVersion() = %v, want 1", result2.FromVersion())
	}
	if result2.ToVersion() != 3 {
		t.Errorf("Second append ToVersion() = %v, want 3", result2.ToVersion())
	}
}

func TestStream_FullWorkflow(t *testing.T) {
	aggregateID := uuid.New().String()

	stream := Stream{
		AggregateType: "User",
		AggregateID:   aggregateID,
		Events: []PersistedEvent{
			{
				AggregateType:    "User",
				AggregateID:      aggregateID,
				AggregateVersion: 1,
				EventType:        "UserCreated",
				GlobalPosition:   100,
				CreatedAt:        time.Now(),
			},
			{
				AggregateType:    "User",
				AggregateID:      aggregateID,
				AggregateVersion: 2,
				EventType:        "UserUpdated",
				GlobalPosition:   101,
				CreatedAt:        time.Now(),
			},
			{
				AggregateType:    "User",
				AggregateID:      aggregateID,
				AggregateVersion: 3,
				EventType:        "UserActivated",
				GlobalPosition:   102,
				CreatedAt:        time.Now(),
			},
		},
	}

	if stream.Version() != 3 {
		t.Errorf("Stream.Version() = %v, want 3", stream.Version())
	}
	if stream.IsEmpty() {
		t.Error("Stream.IsEmpty() = true, want false")
	}
	if stream.Len() != 3 {
		t.Errorf("Stream.Len() = %v, want 3", stream.Len())
	}
}

package store

import (
	"fmt"
	"testing"
)

func TestExpectedVersion_Any(t *testing.T) {
	ev := Any()

	if !ev.IsAny() {
		t.Error("Expected IsAny() to be true")
	}
	if ev.IsNoStream() {
		t.Error("Expected IsNoStream() to be false")
	}
	if ev.IsExact() {
		t.Error("Expected IsExact() to be false")
	}
	if ev.Value() != 0 {
		t.Errorf("Expected Value() to be 0, got %d", ev.Value())
	}
	if ev.String() != "Any" {
		t.Errorf("Expected String() to be 'Any', got '%s'", ev.String())
	}
}

func TestExpectedVersion_NoStream(t *testing.T) {
	ev := NoStream()

	if ev.IsAny() {
		t.Error("Expected IsAny() to be false")
	}
	if !ev.IsNoStream() {
		t.Error("Expected IsNoStream() to be true")
	}
	if ev.IsExact() {
		t.Error("Expected IsExact() to be false")
	}
	if ev.Value() != 0 {
		t.Errorf("Expected Value() to be 0, got %d", ev.Value())
	}
	if ev.String() != "NoStream" {
		t.Errorf("Expected String() to be 'NoStream', got '%s'", ev.String())
	}
}

func TestExpectedVersion_Exact(t *testing.T) {
	tests := []struct {
		name    string
		version int64
	}{
		{"version 1", 1},
		{"version 5", 5},
		{"version 100", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := Exact(tt.version)

			if ev.IsAny() {
				t.Error("Expected IsAny() to be false")
			}
			if ev.IsNoStream() {
				t.Error("Expected IsNoStream() to be false")
			}
			if !ev.IsExact() {
				t.Error("Expected IsExact() to be true")
			}
			if ev.Value() != tt.version {
				t.Errorf("Expected Value() to be %d, got %d", tt.version, ev.Value())
			}
			expectedStr := fmt.Sprintf("Exact(%d)", tt.version)
			if ev.String() != expectedStr {
				t.Errorf("Expected String() to be '%s', got '%s'", expectedStr, ev.String())
			}
		})
	}
}

func TestExpectedVersion_Exact_Panic(t *testing.T) {
	tests := []struct {
		name    string
		version int64
	}{
		{"negative", -1},
		{"large negative", -100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("Expected Exact(%d) to panic", tt.version)
				}
			}()
			Exact(tt.version)
		})
	}
}

func TestExpectedVersion_Exact_Zero(t *testing.T) {
	// Exact(0) should be equivalent to NoStream()
	ev := Exact(0)

	if ev.IsAny() {
		t.Error("Expected IsAny() to be false")
	}
	if !ev.IsNoStream() {
		t.Error("Expected IsNoStream() to be true for Exact(0)")
	}
	if ev.IsExact() {
		t.Error("Expected IsExact() to be false for Exact(0)")
	}
	if ev.Value() != 0 {
		t.Errorf("Expected Value() to be 0, got %d", ev.Value())
	}
	if ev.String() != "NoStream" {
		t.Errorf("Expected String() to be 'NoStream', got '%s'", ev.String())
	}

	// Verify Exact(0) and NoStream() are truly equivalent
	ns := NoStream()
	if ev != ns {
		t.Error("Expected Exact(0) to be equal to NoStream()")
	}
}

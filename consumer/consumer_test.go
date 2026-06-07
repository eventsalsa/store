package consumer

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/eventsalsa/store"
)

// mockGlobalConsumer is a consumer that receives all events
type mockGlobalConsumer struct {
	name           string
	receivedEvents []store.PersistedEvent
}

func (p *mockGlobalConsumer) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *mockGlobalConsumer) Handle(_ context.Context, _ pgx.Tx, event store.PersistedEvent) error {
	p.receivedEvents = append(p.receivedEvents, event)
	return nil
}

// mockScopedConsumer is a consumer that only receives specific aggregate types
type mockScopedConsumer struct {
	name           string
	aggregateTypes []string
	receivedEvents []store.PersistedEvent
}

func (p *mockScopedConsumer) Name() string {
	return p.name
}

func (p *mockScopedConsumer) AggregateTypes() []string {
	return p.aggregateTypes
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *mockScopedConsumer) Handle(_ context.Context, _ pgx.Tx, event store.PersistedEvent) error {
	p.receivedEvents = append(p.receivedEvents, event)
	return nil
}

func TestScopedConsumer_Interface(_ *testing.T) {
	// Test that mockScopedConsumer implements both interfaces
	var _ Consumer = &mockScopedConsumer{}
	var _ ScopedConsumer = &mockScopedConsumer{}

	// Test that mockGlobalConsumer implements only Consumer
	var _ Consumer = &mockGlobalConsumer{}
}

func TestScopedConsumer_TypeAssertion(t *testing.T) {
	globalProj := &mockGlobalConsumer{name: "global"}
	scopedProj := &mockScopedConsumer{name: "scoped", aggregateTypes: []string{"User"}}

	// Global consumer should not be a ScopedConsumer
	if _, ok := Consumer(globalProj).(ScopedConsumer); ok {
		t.Error("Global consumer should not implement ScopedConsumer")
	}

	// Scoped consumer should be a ScopedConsumer
	if _, ok := Consumer(scopedProj).(ScopedConsumer); !ok {
		t.Error("Scoped consumer should implement ScopedConsumer")
	}
}

func TestScopedConsumer_EmptyAggregateTypes(t *testing.T) {
	scopedProj := &mockScopedConsumer{
		name:           "scoped_empty",
		aggregateTypes: []string{},
	}

	types := scopedProj.AggregateTypes()
	if types == nil {
		t.Error("AggregateTypes should not return nil")
	}
	if len(types) != 0 {
		t.Errorf("Expected empty slice, got %v", types)
	}
}

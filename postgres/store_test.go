package postgres

import (
	"strings"
	"testing"

	"github.com/eventsalsa/store"
)

// Compile-time interface compliance checks.
var (
	_ store.EventStore            = (*Store)(nil)
	_ store.EventReader           = (*Store)(nil)
	_ store.GlobalPositionReader  = (*Store)(nil)
	_ store.AggregateStreamReader = (*Store)(nil)
)

func TestWithNotifyChannel(t *testing.T) {
	t.Parallel()

	config := NewStoreConfig(WithNotifyChannel("my_events"))

	if config.NotifyChannel != "my_events" {
		t.Errorf("NotifyChannel = %q, want %q", config.NotifyChannel, "my_events")
	}
}

func TestDefaultStoreConfig_NotifyChannelEmpty(t *testing.T) {
	t.Parallel()

	config := DefaultStoreConfig()

	if config.NotifyChannel != "" {
		t.Errorf("NotifyChannel = %q, want empty (disabled by default)", config.NotifyChannel)
	}
}

func TestBuildReadEventsQuery(t *testing.T) {
	t.Parallel()

	query, args := buildReadEventsQuery("events", 41, 25)
	normalized := strings.Join(strings.Fields(query), " ")

	for _, want := range []string{
		"WHERE global_position > $1",
		"ORDER BY global_position ASC",
		"LIMIT $2",
	} {
		if !strings.Contains(normalized, want) {
			t.Fatalf("query %q missing %q", normalized, want)
		}
	}

	if len(args) != 2 {
		t.Fatalf("len(args) = %d, want 2", len(args))
	}
	if args[0] != int64(41) {
		t.Fatalf("args[0] = %v, want 41", args[0])
	}
	if args[1] != 25 {
		t.Fatalf("args[1] = %v, want 25", args[1])
	}
}

package spannerstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/traego/es"
)

func TestTailCursorUnit(t *testing.T) {
	ctx := context.Background()
	s, _ := unitStore(t)
	for _, tc := range []struct {
		topic string
		p     int
	}{{"", 0}, {"topic", -1}} {
		if _, err := s.GetTailCursor(ctx, tc.topic, tc.p); err == nil {
			t.Fatal("invalid tail scope accepted")
		}
	}
	s, _ = unitStore(t, readAnswer{err: errInjected})
	if _, err := s.GetTailCursor(ctx, "topic", 0); !errors.Is(err, errInjected) {
		t.Fatal("lost topic cause", err)
	}
	s, _ = unitStore(t, answer(t, int64(1)), readAnswer{err: errInjected})
	if _, err := s.GetTailCursor(ctx, "topic", 0); !errors.Is(err, errInjected) {
		t.Fatal("lost tail cause", err)
	}
	s, _ = unitStore(t, answer(t, int64(1)), readAnswer{})
	if tail, err := s.GetTailCursor(ctx, "topic", 0); err != nil || tail != "" {
		t.Fatalf("empty tail: %q %v", tail, err)
	}
	bad, _ := spanner.NewRow([]string{"invalid"}, []any{"wrong shape"})
	s, _ = unitStore(t, answer(t, int64(1)), readAnswer{rows: []*spanner.Row{bad}})
	if _, err := s.GetTailCursor(ctx, "topic", 0); err == nil {
		t.Fatal("malformed tail accepted")
	}
	stamp := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	row, err := spanner.NewRow([]string{"global_timestamp", "stream_name", "version"}, []any{stamp, "stream", int64(3)})
	requireOK(t, err)
	s, f := unitStore(t, answer(t, int64(1)), readAnswer{rows: []*spanner.Row{row}})
	tail, err := s.GetTailCursor(ctx, "topic", 0)
	requireOK(t, err)
	decoded, err := decodeCursor(tail, "topic", 0)
	requireOK(t, err)
	if decoded.Stream != "stream" || decoded.Version != 3 || !decoded.Timestamp.Equal(stamp) {
		t.Fatalf("tail position: %+v", decoded)
	}
	if len(f.statements) != 2 {
		t.Fatal("tail should use metadata queries, not page through history")
	}
}

func TestInvalidCursorErrorParity(t *testing.T) {
	for _, wire := range []string{"!", "eA", "e30", encodeCursor(cursorData{Topic: "other", Partition: 0, Timestamp: time.Now(), Stream: "s", Version: 1})} {
		_, err := decodeCursor(wire, "topic", 0)
		if !errors.Is(err, es.ErrCursorNotFound) {
			t.Fatalf("invalid cursor did not match common sentinel: %v", err)
		}
	}
}

func TestEmulatorSubscriberTailAndReopen(t *testing.T) {
	s, client, ctx := emulatorStore(t)
	requireOK(t, s.EnsureTopic(ctx, "tail-test", 1))
	empty, err := s.GetTailCursor(ctx, "tail-test", 0)
	requireOK(t, err)
	requireOK(t, s.SaveSubscriptionCursor(ctx, "tail-test", "empty-subscriber", 0, empty))
	requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{batchWrite("tail-test", "old", "org", 3)}))
	tail, err := s.GetTailCursor(ctx, "tail-test", 0)
	requireOK(t, err)
	requireOK(t, s.SaveSubscriptionCursor(ctx, "tail-test", "subscriber", 0, tail))
	requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{batchWrite("tail-test", "new", "org", 1)}))
	reopened, err := New(client, Config{})
	requireOK(t, err)
	requireOK(t, reopened.RegisterAggregate(ctx, &testAggregate{topic: "tail-test", count: 1, prototypes: []es.Event{&testEvent{}}}))
	saved, err := reopened.GetSubscriptionCursor(ctx, "tail-test", "subscriber", 0)
	requireOK(t, err)
	page, err := reopened.GetEventsByTopic(ctx, "tail-test", 0, saved)
	requireOK(t, err)
	if len(page.Events) != 1 || page.Events[0].GetStreamName() != "new" {
		t.Fatalf("tail lost append or replayed history: %+v", page)
	}
	saved, err = reopened.GetSubscriptionCursor(ctx, "tail-test", "empty-subscriber", 0)
	requireOK(t, err)
	page, err = reopened.GetEventsByTopic(ctx, "tail-test", 0, saved)
	requireOK(t, err)
	if len(page.Events) != 4 {
		t.Fatal("empty checkpoint was lost on reopen", len(page.Events))
	}
	_, err = reopened.GetSubscriptionCursor(ctx, "tail-test", "missing", 0)
	if !errors.Is(err, es.ErrCursorNotFound) {
		t.Fatal("missing checkpoint", err)
	}
}

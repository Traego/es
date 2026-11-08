package es

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryTailCursor(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "topic")
	empty, err := s.GetTailCursor(ctx, "topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "old", "org", memoryEventFor("old"))}); err != nil {
		t.Fatal(err)
	}
	page, err := s.GetEventsByTopic(ctx, "topic", 0, empty)
	if err != nil || len(page.Events) != 1 {
		t.Fatalf("empty tail lost first append: %+v, %v", page, err)
	}
	tail, err := s.GetTailCursor(ctx, "topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "new", "org", memoryEventFor("new"))}); err != nil {
		t.Fatal(err)
	}
	page, err = s.GetEventsByTopic(ctx, "topic", 0, tail)
	if err != nil || len(page.Events) != 1 || page.Events[0].GetEvent().GetAggregateId() != "new" {
		t.Fatalf("tail replayed history or lost append: %+v, %v", page, err)
	}
	for _, tc := range []struct {
		topic     string
		partition int
	}{{"missing", 0}, {"topic", -1}, {"topic", 1}} {
		if _, err := s.GetTailCursor(ctx, tc.topic, tc.partition); err == nil {
			t.Fatal("invalid tail scope accepted", tc)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.GetTailCursor(canceled, "topic", 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSubscriberInitialPositionAndCheckpointPrecedence(t *testing.T) {
	for _, position := range []StartPosition{StartBeginning, StartTail} {
		s := memoryStoreFor(t, "topic")
		ctx := context.Background()
		if err := s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "old", "org", memoryEventFor("old"))}); err != nil {
			t.Fatal(err)
		}
		sp := NewStreamProcessor(s, "subscriber", "topic", []int{0}, true, func(Envelope) error { return nil }, WithStartPosition(position)).(*streamProcessor)
		cursor, err := sp.initialCursor(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		saved, err := s.GetSubscriptionCursor(ctx, "topic", "subscriber", 0)
		if err != nil || saved != cursor {
			t.Fatalf("initial checkpoint not persisted: %q %v", saved, err)
		}
		// Restart before processing a page. A changed policy cannot skip a later event.
		if err := s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "new", "org", memoryEventFor("new"))}); err != nil {
			t.Fatal(err)
		}
		restarted := NewStreamProcessor(s, "subscriber", "topic", []int{0}, true, func(Envelope) error { return nil }, WithStartPosition(StartTail)).(*streamProcessor)
		resumed, err := restarted.initialCursor(ctx, 0)
		if err != nil || resumed != cursor {
			t.Fatalf("saved cursor replaced by policy: %q %v", resumed, err)
		}
		page, err := s.GetEventsByTopic(ctx, "topic", 0, resumed)
		want := 2
		if position == StartTail {
			want = 1
		}
		if err != nil || len(page.Events) != want {
			t.Fatalf("position %v: %d events, %v", position, len(page.Events), err)
		}
		// A deliberately saved empty cursor means replay, even with tail configured.
		if err := s.SaveSubscriptionCursor(ctx, "topic", "subscriber", 0, ""); err != nil {
			t.Fatal(err)
		}
		reset, err := restarted.initialCursor(ctx, 0)
		if err != nil || reset != "" {
			t.Fatalf("explicit reset lost: %q %v", reset, err)
		}
	}
}

func TestSubscriberTailOnEmptyTopicIsPersisted(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "topic")
	sp := NewStreamProcessor(s, "subscriber", "topic", []int{0}, true, func(Envelope) error { return nil }, WithStartPosition(StartTail)).(*streamProcessor)
	initial, err := sp.initialCursor(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "new", "org", memoryEventFor("new"))}); err != nil {
		t.Fatal(err)
	}
	resumed, err := sp.initialCursor(ctx, 0)
	if err != nil || resumed != initial {
		t.Fatalf("empty initial tail was recaptured: %q %v", resumed, err)
	}
	page, err := s.GetEventsByTopic(ctx, "topic", 0, resumed)
	if err != nil || len(page.Events) != 1 {
		t.Fatalf("first event lost after restart: %+v %v", page, err)
	}
}

type initialPositionStore struct {
	EventStore
	load func() (string, error)
	tail func() (string, error)
	save func(string) error
}

func (s initialPositionStore) GetSubscriptionCursor(context.Context, string, string, int) (string, error) {
	return s.load()
}
func (s initialPositionStore) GetTailCursor(context.Context, string, int) (string, error) {
	return s.tail()
}
func (s initialPositionStore) SaveSubscriptionCursor(_ context.Context, _, _ string, _ int, c string) error {
	return s.save(c)
}

func TestSubscriberInitializationFailuresAndEphemeralTail(t *testing.T) {
	failure := errors.New("storage unavailable")
	for _, stage := range []string{"load", "tail", "save", "ephemeral"} {
		captured, saved := 0, 0
		store := initialPositionStore{
			load: func() (string, error) {
				if stage == "load" {
					return "", failure
				}
				return "", ErrCursorNotFound
			},
			tail: func() (string, error) {
				captured++
				if stage == "tail" {
					return "", failure
				}
				return "tail", nil
			},
			save: func(c string) error {
				saved++
				if c != "tail" {
					t.Errorf("wrong baseline: %q", c)
				}
				return failure
			},
		}
		sp := NewStreamProcessor(store, "subscriber", "topic", []int{0}, stage != "ephemeral", func(Envelope) error { return nil }, WithStartPosition(StartTail), WithSystemRetryConfig(RetryConfig{})).(*streamProcessor)
		cursor, err := sp.initialCursor(context.Background(), 0)
		if stage == "ephemeral" {
			if err != nil || cursor != "tail" || captured != 1 || saved != 0 {
				t.Fatalf("ephemeral initialization: %q %v %d %d", cursor, err, captured, saved)
			}
		} else if !errors.Is(err, failure) {
			t.Fatalf("%s lost cause: %v", stage, err)
		}
	}
	sp := NewStreamProcessor(memoryStoreFor(t, "topic"), "subscriber", "topic", []int{0}, true, func(Envelope) error { return nil }, WithStartPosition(StartPosition(99)))
	if errs := processorErrors(t, sp.Start()); len(errs) != 1 {
		t.Fatal("invalid start position accepted", errs)
	}
}

func TestSubscriberTailProcessing(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "topic")
	if err := s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "old", "org", memoryEventFor("old"))}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	got := make(chan Envelope, 2)
	store := initialPositionStore{EventStore: s, load: func() (string, error) { return s.GetSubscriptionCursor(ctx, "topic", "subscriber", 0) }, tail: func() (string, error) { return s.GetTailCursor(ctx, "topic", 0) }, save: func(c string) error {
		if err := s.SaveSubscriptionCursor(ctx, "topic", "subscriber", 0, c); err != nil {
			return err
		}
		select {
		case <-started:
		default:
			close(started)
		}
		return nil
	}}
	sp := NewStreamProcessor(store, "subscriber", "topic", []int{0}, true, func(e Envelope) error { got <- e; return nil }, WithStartPosition(StartTail), WithNewMessagePollingTime(time.Millisecond))
	ch := sp.Start()
	defer sp.Stop()
	processorSignal(t, started)
	if err := s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "new", "org", memoryEventFor("new"))}); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-got:
		if e.GetEvent().GetAggregateId() != "new" {
			t.Fatal("history replayed", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("new event not delivered")
	}
	sp.Stop()
	if errs := processorErrors(t, ch); len(errs) != 0 {
		t.Fatal(errs)
	}
}

func TestMaterializerStartPositionOption(t *testing.T) {
	m := NewMaterializer("subscriber", "topic", []int{0}, nil, nil, nil, true, WithMaterializerStartPosition(StartTail)).(*materializer)
	if m.streamProcessor.(*streamProcessor).startPosition != StartTail {
		t.Fatal("materializer did not forward start policy")
	}
}

func TestMemorySavedCursorValidation(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "topic")
	for _, cursor := range []string{"!", "eA", memoryCursorFor("other", 0, 0), memoryCursorFor("topic", 1, 0)} {
		if err := s.SaveSubscriptionCursor(ctx, "topic", "subscriber", 0, cursor); !errors.Is(err, ErrCursorNotFound) {
			t.Fatalf("invalid checkpoint accepted: %v", err)
		}
	}
	if _, err := s.GetSubscriptionCursor(ctx, "topic", "subscriber", 0); !errors.Is(err, ErrCursorNotFound) {
		t.Fatal("invalid checkpoint persisted", err)
	}
}

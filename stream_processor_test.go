package es

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type processorTestStore struct {
	EventStore
	fetch func(context.Context, int, string) (EventsPage, error)
	load  func(context.Context, int) (string, error)
	save  func(context.Context, int, string) error
}

func (s processorTestStore) GetEventsByTopic(ctx context.Context, _ string, p int, c string) (EventsPage, error) {
	return s.fetch(ctx, p, c)
}
func (s processorTestStore) GetSubscriptionCursor(ctx context.Context, _, _ string, p int) (string, error) {
	if s.load != nil {
		return s.load(ctx, p)
	}
	return "", ErrCursorNotFound
}
func (s processorTestStore) SaveSubscriptionCursor(ctx context.Context, _, _ string, p int, c string) error {
	if s.save != nil {
		return s.save(ctx, p, c)
	}
	return nil
}
func processorTestOptions() []StreamProcessorOption {
	return []StreamProcessorOption{WithNewMessagePollingTime(time.Millisecond), WithSystemRetryConfig(RetryConfig{}), WithProcessEventRetryConfig(RetryConfig{})}
}
func processorErrors(t *testing.T, ch <-chan error) []error {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	var result []error
	for {
		select {
		case err, ok := <-ch:
			if !ok {
				return result
			}
			result = append(result, err)
		case <-timer.C:
			t.Fatal("processor error channel did not close")
		}
	}
}
func processorSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("processor did not reach expected state")
	}
}

func TestStreamProcessorConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*streamProcessor)
	}{
		{"context", func(s *streamProcessor) { s.parent = nil }},
		{"store", func(s *streamProcessor) { s.eventStore = nil }},
		{"handler", func(s *streamProcessor) { s.handler = nil }},
		{"topic", func(s *streamProcessor) { s.topic = "" }},
		{"subscriber", func(s *streamProcessor) { s.subscriberId = "" }},
		{"polling", func(s *streamProcessor) { s.pollingTime = 0 }},
		{"partitions", func(s *streamProcessor) { s.partitions = nil }},
		{"negative partition", func(s *streamProcessor) { s.partitions = []int{-1} }},
		{"duplicate partition", func(s *streamProcessor) { s.partitions = []int{0, 0} }},
		{"system retry", func(s *streamProcessor) { s.systemRetryConfig = RetryConfig{MaxRetryDuration: -1} }},
		{"handler retry", func(s *streamProcessor) { s.processEventRetryConfig = RetryConfig{MaxRetryDuration: time.Second} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sp := NewStreamProcessorWithContext(context.Background(), processorTestStore{}, "sub", "topic", []int{0}, true, func(context.Context, Envelope) error { return nil }, nil).(*streamProcessor)
			tc.edit(sp)
			sp.Stop()
			if errs := processorErrors(t, sp.Start()); len(errs) != 1 || errs[0] == nil {
				t.Fatalf("configuration failure was not returned: %v", errs)
			}
			sp.Stop()
		})
	}
	if errs := processorErrors(t, NewStreamProcessor(processorTestStore{}, "sub", "topic", []int{0}, false, nil).Start()); len(errs) != 1 {
		t.Fatalf("nil legacy handler accepted: %v", errs)
	}
}

func TestStreamProcessorFatalFailuresCloseAndRestart(t *testing.T) {
	failure := errors.New("test failure")
	for _, source := range []string{"load", "fetch", "handler", "panic", "save"} {
		t.Run(source, func(t *testing.T) {
			var saves, calls atomic.Int32
			store := processorTestStore{
				load: func(context.Context, int) (string, error) {
					if source == "load" {
						return "", failure
					}
					return "before", nil
				},
				fetch: func(context.Context, int, string) (EventsPage, error) {
					if source == "fetch" {
						return EventsPage{}, failure
					}
					return EventsPage{Events: []Envelope{{version: 1}, {version: 2}}, NextCursor: "after"}, nil
				},
				save: func(context.Context, int, string) error { saves.Add(1); return failure },
			}
			sp := NewStreamProcessor(store, "sub", "topic", []int{0}, true, func(Envelope) error {
				calls.Add(1)
				if source == "handler" {
					return failure
				}
				if source == "panic" {
					panic("handler failed")
				}
				return nil
			}, processorTestOptions()...)
			for attempt := 0; attempt < 2; attempt++ {
				errs := processorErrors(t, sp.Start())
				if len(errs) != 1 {
					t.Fatalf("expected one reported fatal error: %v", errs)
				}
				if source == "panic" {
					if !strings.Contains(errs[0].Error(), "panic processing event") {
						t.Fatal(errs[0])
					}
				} else if !errors.Is(errs[0], failure) {
					t.Fatalf("lost cause: %v", errs[0])
				}
				sp.Stop()
			}
			if source != "save" && saves.Load() != 0 {
				t.Fatal("failed page checkpointed")
			}
			if (source == "handler" || source == "panic") && calls.Load() != 2 {
				t.Fatal("processing continued past failed event")
			}
		})
	}
}

func TestStreamProcessorConcurrentLifecycleAndPartitionCopy(t *testing.T) {
	entered := make(chan struct{}, 2)
	var fetches atomic.Int32
	store := processorTestStore{fetch: func(ctx context.Context, p int, _ string) (EventsPage, error) {
		if p != 2 {
			return EventsPage{}, fmt.Errorf("mutated partition: %d", p)
		}
		fetches.Add(1)
		entered <- struct{}{}
		<-ctx.Done()
		return EventsPage{}, ctx.Err()
	}}
	partitions := []int{2}
	sp := NewStreamProcessor(store, "", "topic", partitions, false, func(Envelope) error { return nil }, processorTestOptions()...)
	partitions[0] = 99
	sp.Stop()
	var previous <-chan error
	for run := 0; run < 2; run++ {
		ch := sp.Start()
		if ch == previous {
			t.Fatal("restart reused completed channel")
		}
		previous = ch
		processorSignal(t, entered)
		var starts sync.WaitGroup
		for i := 0; i < 20; i++ {
			starts.Add(1)
			go func() {
				defer starts.Done()
				if sp.Start() != ch {
					t.Error("Start was not idempotent")
				}
			}()
		}
		starts.Wait()
		var stops sync.WaitGroup
		for i := 0; i < 20; i++ {
			stops.Add(1)
			go func() { defer stops.Done(); sp.Stop() }()
		}
		stops.Wait()
		if errs := processorErrors(t, ch); len(errs) != 0 {
			t.Fatalf("shutdown reported failure: %v", errs)
		}
	}
	if fetches.Load() != 2 {
		t.Fatalf("duplicate workers launched: %d", fetches.Load())
	}
}

func TestStreamProcessorCheckpointAfterWholePage(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			var handled, saved atomic.Int32
			continued := make(chan struct{})
			firstCursor := ""
			if existing {
				firstCursor = "existing"
			}
			store := processorTestStore{
				load: func(context.Context, int) (string, error) {
					if existing {
						return firstCursor, nil
					}
					return "ignored", fmt.Errorf("missing: %w", ErrCursorNotFound)
				},
				fetch: func(ctx context.Context, _ int, c string) (EventsPage, error) {
					if c == firstCursor {
						return EventsPage{Events: []Envelope{{version: 1}, {version: 2}}, NextCursor: "after"}, nil
					}
					if c != "after" {
						return EventsPage{}, fmt.Errorf("unexpected cursor %q", c)
					}
					close(continued)
					<-ctx.Done()
					return EventsPage{}, ctx.Err()
				},
				save: func(_ context.Context, _ int, c string) error {
					if handled.Load() != 2 || c != "after" {
						return errors.New("checkpoint before completed page")
					}
					saved.Add(1)
					return nil
				},
			}
			sp := NewStreamProcessor(store, "sub", "topic", []int{0}, true, func(e Envelope) error {
				if int(handled.Add(1)) != e.version {
					return errors.New("events processed out of order")
				}
				return nil
			}, processorTestOptions()...)
			ch := sp.Start()
			processorSignal(t, continued)
			sp.Stop()
			if errs := processorErrors(t, ch); len(errs) != 0 {
				t.Fatal(errs)
			}
			if saved.Load() != 1 {
				t.Fatalf("checkpoint count %d", saved.Load())
			}
		})
	}
}

func TestStreamProcessorContextCancelsHandler(t *testing.T) {
	entered := make(chan struct{})
	var saved atomic.Bool
	store := processorTestStore{fetch: func(context.Context, int, string) (EventsPage, error) {
		return EventsPage{Events: []Envelope{{}, {}}, NextCursor: "after"}, nil
	}, save: func(context.Context, int, string) error { saved.Store(true); return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sp := NewStreamProcessorWithContext(ctx, store, "sub", "topic", []int{0}, true, func(ctx context.Context, _ Envelope) error { close(entered); <-ctx.Done(); return ctx.Err() }, processorTestOptions()...)
	ch := sp.Start()
	processorSignal(t, entered)
	cancel()
	if errs := processorErrors(t, ch); len(errs) != 0 {
		t.Fatal(errs)
	}
	if saved.Load() {
		t.Fatal("canceled page checkpointed")
	}
	if errs := processorErrors(t, sp.Start()); len(errs) != 0 {
		t.Fatal(errs)
	}
	sp.Stop()
}

func TestStreamProcessorIdleCancellationAndUnchangedCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var fetches atomic.Int32
	store := processorTestStore{
		load: func(context.Context, int) (string, error) { return "same", nil },
		fetch: func(context.Context, int, string) (EventsPage, error) {
			if fetches.Add(1) == 2 {
				cancel()
			}
			return EventsPage{NextCursor: "same"}, nil
		},
		save: func(context.Context, int, string) error { t.Error("unchanged cursor saved"); return nil },
	}
	sp := NewStreamProcessorWithContext(ctx, store, "sub", "topic", []int{0}, true, func(context.Context, Envelope) error { return nil }, processorTestOptions()...)
	if errs := processorErrors(t, sp.Start()); len(errs) != 0 {
		t.Fatal(errs)
	}
	if fetches.Load() != 2 {
		t.Fatal("processor did not poll again")
	}
}

func TestStreamProcessorRetriesWithoutSkippingEvents(t *testing.T) {
	var fetches, calls int
	store := processorTestStore{fetch: func(context.Context, int, string) (EventsPage, error) {
		fetches++
		if fetches == 1 {
			return EventsPage{}, errors.New("transient read")
		}
		return EventsPage{Events: []Envelope{{version: 1}, {version: 2}}, NextCursor: "after"}, nil
	}}
	sp := NewStreamProcessor(store, "sub", "topic", []int{0}, false, func(e Envelope) error {
		calls++
		if calls == 1 {
			panic("transient panic")
		}
		if calls-1 != e.version {
			return errors.New("event order changed during retry")
		}
		return nil
	}, WithSystemRetryConfig(RetryConfig{time.Second, time.Nanosecond}), WithProcessEventRetryConfig(RetryConfig{time.Second, time.Nanosecond})).(*streamProcessor)
	next, err := sp.processNewEvents(context.Background(), 0, "before")
	if err != nil || next != "after" || calls != 3 || fetches != 2 {
		t.Fatalf("next=%q err=%v calls=%d fetches=%d", next, err, calls, fetches)
	}
}

func TestStreamProcessorFatalCancelsSiblingPartitionsWithoutErrorConsumer(t *testing.T) {
	failure := errors.New("partition failed")
	entered, exited := make(chan struct{}, 3), make(chan struct{}, 3)
	fail := make(chan struct{})
	store := processorTestStore{fetch: func(ctx context.Context, partition int, _ string) (EventsPage, error) {
		entered <- struct{}{}
		defer func() { exited <- struct{}{} }()
		if partition == 0 {
			<-fail
			return EventsPage{}, failure
		}
		<-ctx.Done()
		return EventsPage{}, ctx.Err()
	}}
	sp := NewStreamProcessor(store, "sub", "topic", []int{0, 1, 2}, false, func(Envelope) error { return nil }, processorTestOptions()...)
	ch := sp.Start()
	for i := 0; i < 3; i++ {
		processorSignal(t, entered)
	}
	close(fail)
	// Nobody reads the error channel until all three store calls have returned.
	for i := 0; i < 3; i++ {
		processorSignal(t, exited)
	}
	sp.Stop()
	errs := processorErrors(t, ch)
	if len(errs) != 1 || !errors.Is(errs[0], failure) {
		t.Fatalf("fatal error was not delivered: %v", errs)
	}
}

func TestStreamProcessorStartDuringStopWaitsForCurrentHandler(t *testing.T) {
	entered, canceled, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	store := processorTestStore{fetch: func(context.Context, int, string) (EventsPage, error) { return EventsPage{Events: []Envelope{{}}}, nil }}
	sp := NewStreamProcessorWithContext(context.Background(), store, "sub", "topic", []int{0}, false, func(ctx context.Context, _ Envelope) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	}, processorTestOptions()...)
	ch := sp.Start()
	processorSignal(t, entered)
	go func() { sp.Stop(); close(stopped) }()
	processorSignal(t, canceled)
	if sp.Start() != ch {
		t.Error("Start launched a replacement before the active handler returned")
	}
	select {
	case <-stopped:
		t.Error("Stop returned while a handler was active")
	default:
	}
	close(release)
	processorSignal(t, stopped)
	if errs := processorErrors(t, ch); len(errs) != 0 {
		t.Fatal(errs)
	}
}

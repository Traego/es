package es

import (
	"context"
	"errors"
	"testing"
)

// Typed-nil interface values must be rejected just like untyped nils, before
// launching a worker or calling a method on the missing dependency.
func TestCoreValidationTypedNilDependencies(t *testing.T) {
	var eventStore *InMemoryEventStore
	t.Run("processor", func(t *testing.T) {
		sp := NewStreamProcessor(eventStore, "sub", "topic", []int{0}, true, func(Envelope) error { return nil })
		errs := processorErrors(t, sp.Start())
		if len(errs) != 1 {
			t.Fatalf("expected configuration error, got %v", errs)
		}
		sp.Stop()
	})
	t.Run("aggregate load", func(t *testing.T) {
		if err := NewAggregateStore(eventStore).Load(context.Background(), "org", "id", NewTestAggregate()); err == nil {
			t.Fatal("typed-nil event store was accepted")
		}
	})
	t.Run("aggregate save", func(t *testing.T) {
		if err := NewAggregateStore(eventStore).SaveBatch(context.Background(), nil); err == nil {
			t.Fatal("typed-nil event store was accepted")
		}
	})
	t.Run("materializer constructor", func(t *testing.T) {
		var aggregateStore *EventSourcedAggregateStore
		m := NewMaterializer("sub", "topic", []int{0}, func(context.Context, AggregateRoot, int) error { return nil }, NewTestAggregate(), aggregateStore, true)
		if errs := processorErrors(t, m.Start()); len(errs) != 1 {
			t.Fatalf("expected configuration error, got %v", errs)
		}
		m.Stop()
	})
	t.Run("materializer event", func(t *testing.T) {
		var aggregateStore *EventSourcedAggregateStore
		m := &materializer{aggregateType: NewTestAggregate(), aggregateStore: aggregateStore, handler: func(context.Context, AggregateRoot, int) error {
			t.Fatal("invalid materializer invoked handler")
			return nil
		}}
		if err := m.processEvent(context.Background(), Envelope{event: &TestAggregateCreated{}}); err == nil {
			t.Fatal("typed-nil aggregate store was accepted")
		}
	})
}

type validationCancelAggregate struct {
	*TestAggregate
	cancel context.CancelFunc
	calls  int
}

func (a *validationCancelAggregate) HandleEvent(context.Context, Event, *Envelope) error {
	a.calls++
	a.cancel()
	return nil
}

func TestCoreValidationReplayCancellation(t *testing.T) {
	for _, count := range []int{1, 2} {
		ctx, cancel := context.WithCancel(context.Background())
		a := &validationCancelAggregate{TestAggregate: NewTestAggregate(), cancel: cancel}
		events := []Envelope{{version: 1, event: &TestAggregateCreated{}}, {version: 2, event: &TestAggregateActivated{}}}
		err := a.LoadHistoricEvents(ctx, events[:count], a)
		cancel()
		if !errors.Is(err, context.Canceled) || a.calls != 1 || a.Version != 1 {
			t.Fatalf("count=%d replay ignored cancellation: err=%v calls=%d version=%d", count, err, a.calls, a.Version)
		}
	}
}

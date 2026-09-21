package es_test

import (
	"context"
	"fmt"

	"github.com/traego/es"
)

type counter struct {
	es.AggregateBase
	Value int
}

type incremented struct {
	ID, Org string
	Amount  int
}

func (*incremented) GetEventType() string         { return "counter.incremented.v1" }
func (e *incremented) GetAggregateId() string     { return e.ID }
func (e *incremented) GetOrgId() string           { return e.Org }
func (*incremented) GetEventCodec() es.EventCodec { return es.JsonEventCodec{} }

func (*counter) GetAggregateType() string               { return "counter" }
func (*counter) GetTopic() string                       { return "counters" }
func (*counter) GetPartitionCount() int                 { return 4 }
func (*counter) InitialState() es.AggregateRoot         { return &counter{} }
func (*counter) RegisterEvents(register func(es.Event)) { register(&incremented{}) }
func (c *counter) HandleEvent(_ context.Context, event es.Event, _ *es.Envelope) error {
	e, ok := event.(*incremented)
	if !ok {
		return fmt.Errorf("unsupported event %T", event)
	}
	c.Id, c.OrgId = e.ID, e.Org
	c.Value += e.Amount
	return nil
}
func (c *counter) HandleCommand(ctx context.Context, command any) ([]es.Event, error) {
	amount, ok := command.(int)
	if !ok || amount <= 0 {
		return nil, fmt.Errorf("increment must be a positive integer")
	}
	event := &incremented{ID: c.Id, Org: c.OrgId, Amount: amount}
	if err := es.ApplyEvent(ctx, c, event); err != nil {
		return nil, err
	}
	return []es.Event{event}, nil
}

func Example() {
	ctx := context.Background()
	events := es.NewInMemoryEventStore()
	if err := events.RegisterAggregate(ctx, &counter{}); err != nil {
		panic(err)
	}
	aggregates := es.NewAggregateStore(events)
	current := &counter{AggregateBase: es.AggregateBase{Id: "counter-1", OrgId: "org-1"}}
	if _, err := current.HandleCommand(ctx, 3); err != nil {
		panic(err)
	}
	if err := aggregates.Save(ctx, current); err != nil {
		panic(err)
	}
	restored := &counter{}
	if err := aggregates.Load(ctx, "org-1", "counter-1", restored); err != nil {
		panic(err)
	}
	fmt.Printf("value=%d version=%d\n", restored.Value, restored.GetVersion())
	// Output: value=3 version=1
}

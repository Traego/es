package sqlitestore

import (
	"context"
	"fmt"
	"testing"

	"github.com/traego/es"
)

type counter struct {
	es.AggregateBase
	Value string
}

func (*counter) GetAggregateType() string               { return "counter" }
func (*counter) GetTopic() string                       { return "topic" }
func (*counter) GetPartitionCount() int                 { return 1 }
func (*counter) InitialState() es.AggregateRoot         { return &counter{} }
func (*counter) RegisterEvents(register func(es.Event)) { register(&event{}) }
func (c *counter) HandleEvent(_ context.Context, e es.Event, _ *es.Envelope) error {
	value, ok := e.(*event)
	if !ok {
		return fmt.Errorf("unexpected event %T", e)
	}
	c.Id = value.ID
	c.OrgId = value.Org
	c.Value = value.Value
	return nil
}
func (c *counter) HandleCommand(ctx context.Context, command any) ([]es.Event, error) {
	e := &event{ID: c.Id, Org: c.OrgId, Value: command.(string)}
	if err := es.ApplyEvent(ctx, c, e); err != nil {
		return nil, err
	}
	return []es.Event{e}, nil
}

func TestAggregateSaveBatchAndReplayAfterReopen(t *testing.T) {
	ctx := context.Background()
	s, path := openTest(t, Config{})
	must(t, s.RegisterAggregate(ctx, &counter{}))
	a := &counter{AggregateBase: es.AggregateBase{Id: "a", OrgId: "home"}}
	b := &counter{AggregateBase: es.AggregateBase{Id: "b", OrgId: "home"}}
	_, err := a.HandleCommand(ctx, "on")
	must(t, err)
	_, err = b.HandleCommand(ctx, "off")
	must(t, err)
	must(t, es.NewAggregateStore(s).SaveBatch(ctx, []es.AggregateRoot{a, b}))
	must(t, s.Close())
	reopened, err := Open(ctx, path, Config{})
	must(t, err)
	defer reopened.Close()
	must(t, reopened.RegisterAggregate(ctx, &counter{}))
	for _, tc := range []struct{ id, value string }{{"a", "on"}, {"b", "off"}} {
		restored := &counter{}
		must(t, es.NewAggregateStore(reopened).Load(ctx, "home", tc.id, restored))
		if restored.Value != tc.value || restored.GetVersion() != 1 {
			t.Fatalf("replay: %+v", restored)
		}
	}
}

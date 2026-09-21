package es

import (
	"context"
	"testing"
)

type routingOtherAggregate struct{ *TestAggregate }

func (*routingOtherAggregate) GetAggregateType() string { return "OtherAggregate" }

func TestMaterializerRoutesCanonicalStreamsOnSharedTopic(t *testing.T) {
	ctx := context.Background()
	events := testMemoryStore(t)
	other := &routingOtherAggregate{NewTestAggregate()}
	if err := events.RegisterAggregate(ctx, other); err != nil {
		t.Fatal(err)
	}
	aggregates := NewAggregateStore(events)
	created := &TestAggregateCreated{AggregateId: "shared/id", OrgId: "org/one"}
	if err := ApplyEvent(ctx, other, created); err != nil {
		t.Fatal(err)
	}
	if err := aggregates.Save(ctx, other); err != nil {
		t.Fatal(err)
	}
	otherStream, err := GetStreamNameFromFullAgg(other)
	if err != nil {
		t.Fatal(err)
	}
	otherHistory, err := events.GetEvents(ctx, other.GetTopic(), otherStream, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	m := &materializer{
		aggregateType: NewTestAggregate(), aggregateStore: aggregates,
		handler: func(_ context.Context, root AggregateRoot, version int) error {
			calls++
			if root.GetId() != created.AggregateId || root.GetOrgId() != created.OrgId || version != 1 {
				t.Errorf("wrong aggregate projected: id=%s org=%s version=%d", root.GetId(), root.GetOrgId(), version)
			}
			return nil
		},
	}
	// An unrelated stream must not try loading a nonexistent target aggregate.
	if err := m.processEvent(ctx, otherHistory[0]); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("unrelated aggregate was projected")
	}
	target := NewTestAggregate()
	if err := ApplyEvent(ctx, target, created); err != nil {
		t.Fatal(err)
	}
	if err := aggregates.Save(ctx, target); err != nil {
		t.Fatal(err)
	}
	// Even when identifiers collide, aggregate type is part of stream identity.
	if err := m.processEvent(ctx, otherHistory[0]); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("unrelated event projected same-ID target")
	}
	targetStream, err := GetStreamNameFromFullAgg(target)
	if err != nil {
		t.Fatal(err)
	}
	history, err := events.GetEvents(ctx, target.GetTopic(), targetStream, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.processEvent(ctx, history[0]); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("own aggregate projection count %d", calls)
	}
}

func TestMaterializerRejectsInvalidEventIdentity(t *testing.T) {
	for _, tc := range []struct{ name, org, id string }{{name: "missing aggregate ID", org: "org"}, {name: "missing organization", id: "id"}} {
		t.Run(tc.name, func(t *testing.T) {
			m := &materializer{
				aggregateType: NewTestAggregate(), aggregateStore: NewAggregateStore(testMemoryStore(t)),
				handler: func(context.Context, AggregateRoot, int) error {
					t.Fatal("invalid event reached projection handler")
					return nil
				},
			}
			err := m.processEvent(context.Background(), Envelope{orgId: tc.org, streamName: "irrelevant", event: &TestAggregateCreated{AggregateId: tc.id}})
			if err == nil {
				t.Fatal("invalid event identity was accepted")
			}
		})
	}
}

package es

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
)

// AggregateStore loads aggregate history and atomically persists pending events.
// Aggregate instances are single-owner objects; do not mutate one concurrently.
type AggregateStore interface {
	Load(ctx context.Context, orgId, aggregateId string, aggregateRoot AggregateRoot) error
	Save(ctx context.Context, aggregateRoot AggregateRoot) error
	SaveBatch(ctx context.Context, aggregateRoots []AggregateRoot) error
}

// EventSourcedAggregateStore implements AggregateStore on an EventStore.
type EventSourcedAggregateStore struct{ eventStore EventStore }

// NewAggregateStore wraps a store. A nil store is rejected by Load and Save.
func NewAggregateStore(eventStore EventStore) *EventSourcedAggregateStore {
	return &EventSourcedAggregateStore{eventStore: eventStore}
}

// Load replays history into a fresh aggregate. It does not reset existing state.
func (s *EventSourcedAggregateStore) Load(ctx context.Context, orgId, aggregateId string, root AggregateRoot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNil(s.eventStore) || isNil(root) {
		return errors.New("event store and aggregate are required")
	}
	stream, err := GetStreamName(root, orgId, aggregateId)
	if err != nil {
		return err
	}
	if root.GetVersion() != 0 || len(root.getBase().uncommittedEvents) != 0 {
		return errors.New("load requires a fresh aggregate")
	}
	events, err := s.eventStore.GetEvents(ctx, root.GetTopic(), stream, 0, nil)
	if err != nil {
		return NewEventLoadError(aggregateId, root.GetTopic(), err)
	}
	if len(events) == 0 {
		return ErrAggregateNotFound
	}
	return root.LoadHistoricEvents(ctx, events, root)
}

// Save commits the aggregate's pending events with optimistic concurrency.
// On failure the pending events remain available; reload before retrying a
// command after a version conflict, rather than blindly retrying stale events.
func (s *EventSourcedAggregateStore) Save(ctx context.Context, root AggregateRoot) error {
	err := s.SaveBatch(ctx, []AggregateRoot{root})
	if err != nil && !isNil(root) {
		return NewEventSaveError(root.GetId(), len(root.getBase().uncommittedEvents), err)
	}
	return err
}

// SaveBatch commits all pending events atomically. Every aggregate's previous
// persisted version must match, including version zero for a new aggregate.
func (s *EventSourcedAggregateStore) SaveBatch(ctx context.Context, roots []AggregateRoot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNil(s.eventStore) {
		return errors.New("event store is required")
	}
	writes := make([]WriteEvents, 0, len(roots))
	correlation := CorrelationID(ctx)
	if correlation == "" {
		correlation = rand.Text()
	}
	for _, root := range roots {
		if isNil(root) {
			return errors.New("aggregate is required")
		}
		stream, err := GetStreamNameFromFullAgg(root)
		if err != nil {
			return err
		}
		pending := root.getBase().uncommittedEvents
		if len(pending) == 0 {
			continue
		}
		expected := root.GetVersion() - len(pending)
		if expected < 0 {
			return fmt.Errorf("aggregate %q version is smaller than its pending event count", root.GetId())
		}
		writes = append(writes, WriteEvents{Topic: root.GetTopic(), OrgId: root.GetOrgId(), StreamName: stream, PartitionKey: stream, ExpectedVersion: &expected, Events: pending, CorrelationId: correlation})
	}
	if len(writes) == 0 {
		return nil
	}
	if err := s.eventStore.SaveEvents(ctx, writes); err != nil {
		return err
	}
	for _, root := range roots {
		root.getBase().ClearUncommittedEvents()
	}
	return nil
}

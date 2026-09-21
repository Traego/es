package es

import (
	"context"
	"fmt"
	"time"
)

type MaterializerFunc func(ctx context.Context, aggregateRoot AggregateRoot, version int) error

type Materializer interface {
	Start() <-chan error
	Stop()
}

type materializer struct {
	handler         MaterializerFunc
	aggregateStore  AggregateStore
	aggregateType   AggregateRoot
	streamProcessor StreamProcessor
	pollingTime     time.Duration
}

type MaterializerOption func(*materializer, *EventStore)

func WithMaterializerEventStore(eventStore EventStore) MaterializerOption {
	return func(m *materializer, es *EventStore) {
		*es = eventStore
	}
}

func WithMaterializerPollingTime(duration time.Duration) MaterializerOption {
	return func(m *materializer, es *EventStore) {
		m.pollingTime = duration
	}
}

// NewMaterializer constructs a current-state projection consumer.
// The handler can observe a version newer than the triggering event and must
// be idempotent. Each matching notification loads full aggregate history.
// Only canonical streams for aggregateType are consumed; other aggregate
// streams on a shared topic are ignored.
func NewMaterializer(subscriberId, topic string, partitions []int, handler MaterializerFunc, aggregateType AggregateRoot, aggregateStore AggregateStore, persistCursor bool, options ...MaterializerOption) Materializer {
	return NewMaterializerWithContext(context.Background(), subscriberId, topic, partitions, handler, aggregateType, aggregateStore, persistCursor, options...)
}

// NewMaterializerWithContext propagates shutdown cancellation to aggregate
// loading and the projection handler.
func NewMaterializerWithContext(
	parent context.Context,
	subscriberId string,
	topic string,
	partitions []int,
	handler MaterializerFunc,
	aggregateType AggregateRoot,
	aggregateStore AggregateStore,
	persistCursor bool,
	options ...MaterializerOption,
) Materializer {
	m := &materializer{
		handler:        handler,
		aggregateStore: aggregateStore,
		aggregateType:  aggregateType,
	}

	var eventStore EventStore
	for _, option := range options {
		if option != nil {
			option(m, &eventStore)
		}
	}

	if isNil(eventStore) && !isNil(aggregateStore) {
		eventSourcedStore, ok := aggregateStore.(*EventSourcedAggregateStore)
		if ok {
			eventStore = eventSourcedStore.eventStore
		}
	}

	usePollingTime := 100 * time.Millisecond
	if m.pollingTime > 0 {
		usePollingTime = m.pollingTime
	}

	m.streamProcessor = NewStreamProcessorWithContext(
		parent,
		eventStore,
		subscriberId,
		topic,
		partitions,
		persistCursor,
		m.processEvent,
		WithNewMessagePollingTime(usePollingTime),
	)

	return m
}

func (m *materializer) Start() <-chan error {
	return m.streamProcessor.Start()
}

func (m *materializer) Stop() {
	m.streamProcessor.Stop()
}

func (m *materializer) processEvent(ctx context.Context, event Envelope) error {
	if isNil(m.aggregateType) || isNil(m.aggregateStore) || m.handler == nil || isNil(event.GetEvent()) {
		return fmt.Errorf("materializer requires an aggregate type, store, handler, and event")
	}

	aggregateId := event.event.GetAggregateId()
	orgId := event.GetOrgId()
	expectedStream, err := GetStreamName(m.aggregateType, orgId, aggregateId)
	if err != nil {
		return fmt.Errorf("invalid materializer event identity: %w", err)
	}
	if event.GetStreamName() != expectedStream {
		return nil
	}

	aggregateInstance := m.aggregateType.InitialState()

	err = m.aggregateStore.Load(ctx, orgId, aggregateId, aggregateInstance)
	if err != nil {
		return fmt.Errorf("error loading aggregate: %w", err)
	}

	version := aggregateInstance.GetVersion()
	err = m.handler(ctx, aggregateInstance, version)
	if err != nil {
		return fmt.Errorf("error processing event: %w", err)
	}

	return nil
}

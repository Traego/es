package es

import (
	"context"
	"crypto/rand"
	"errors"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockAggregateRoot struct {
	mock.Mock
	AggregateBase
}

func (m *MockAggregateRoot) GetVersion() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockAggregateRoot) GetId() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockAggregateRoot) GetAggregateType() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockAggregateRoot) GetTopic() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockAggregateRoot) InitialState() AggregateRoot {
	args := m.Called()
	return args.Get(0).(AggregateRoot)
}

func (m *MockAggregateRoot) HandleEvent(ctx context.Context, event Event, env *Envelope) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *MockAggregateRoot) HandleCommand(ctx context.Context, command interface{}) ([]Event, error) {
	args := m.Called(ctx, command)
	return args.Get(0).([]Event), args.Error(1)
}

func (m *MockAggregateRoot) RegisterEvents(r func(Event)) {
	m.Called(r)
}

type MockAggregateStore struct {
	mock.Mock
}

func (m *MockAggregateStore) Save(ctx context.Context, aggregateRoot AggregateRoot) error {
	args := m.Called(ctx, aggregateRoot)
	return args.Error(0)
}

func (m *MockAggregateStore) Load(ctx context.Context, orgId, aggregateId string, aggregateRoot AggregateRoot) error {
	args := m.Called(ctx, orgId, aggregateId, aggregateRoot)
	return args.Error(0)
}

func (m *MockAggregateStore) SaveBatch(ctx context.Context, aggregateRoots []AggregateRoot) error {
	args := m.Called(ctx, aggregateRoots)
	return args.Error(0)
}

type MockEventStore struct {
	mock.Mock
}

func (m *MockEventStore) SaveEvents(ctx context.Context, events []WriteEvents) error {
	args := m.Called(ctx, events)
	return args.Error(0)
}

func (m *MockEventStore) GetEvents(ctx context.Context, topic string, streamName string, fromVersion int, limit *int) ([]Event, error) {
	args := m.Called(ctx, topic, streamName, fromVersion, limit)
	return args.Get(0).([]Event), args.Error(1)
}

func (m *MockEventStore) GetEventsByTopic(ctx context.Context, topic string, partitionId int, cursor string) (EventsPage, error) {
	args := m.Called(ctx, topic, partitionId, cursor)
	return args.Get(0).(EventsPage), args.Error(1)
}

func (m *MockEventStore) GetEventsByOrgs(ctx context.Context, topic string, partitionId int, orgIds []string, cursor string) (EventsPage, error) {
	args := m.Called(ctx, topic, partitionId, orgIds, cursor)
	return args.Get(0).(EventsPage), args.Error(1)
}

func (m *MockEventStore) GetLatestEvent(ctx context.Context, topic string, streamName string) (Event, error) {
	args := m.Called(ctx, topic, streamName)
	return args.Get(0).(Event), args.Error(1)
}

type MockStreamProcessor struct {
	mock.Mock
}

func (m *MockStreamProcessor) Start() <-chan error {
	args := m.Called()
	ch := args.Get(0).(chan error)
	return ch
}

func (m *MockStreamProcessor) Stop() {
	m.Called()
}
func TestNewMaterializer(t *testing.T) {
	t.Run("with default event store", func(t *testing.T) {
		ctx := context.Background()
		// Use TestAggregate instead of mock
		testAggregate := NewTestAggregate()
		testAggregate.Id = "test-" + rand.Text()

		// Use a real AggregateStore with InMemoryEventStore
		inMemoryEventStore := testMemoryStore(t)
		err := inMemoryEventStore.RegisterAggregate(ctx, testAggregate)
		require.NoError(t, err)

		aggregateStore := NewAggregateStore(inMemoryEventStore)

		// Ensure the topic exists
		err = inMemoryEventStore.EnsureTopic(context.Background(), testAggregate.GetTopic(), testAggregate.GetPartitionCount())
		require.NoError(t, err)

		materializer := NewMaterializer(
			"test-subscriber",
			testAggregate.GetTopic(),
			[]int{0, 1},
			func(ctx context.Context, aggregateRoot AggregateRoot, version int) error {
				return nil
			},
			testAggregate,
			aggregateStore,
			true,
		)

		// We can only test that the materializer is created successfully
		assert.NotNil(t, materializer, "Materializer should not be nil")

		// Verify that the interface methods exist
		assert.NotPanics(t, func() {
			_ = materializer.Start()
			materializer.Stop()
		}, "Materializer should implement Start and Stop methods")
	})

	t.Run("with custom event store", func(t *testing.T) {
		ctx := context.Background()

		// Use TestAggregate instead of mock
		testAggregate := NewTestAggregate()
		testAggregate.Id = "test-" + rand.Text()

		// Use a real AggregateStore with InMemoryEventStore
		inMemoryEventStore := testMemoryStore(t)
		err := inMemoryEventStore.RegisterAggregate(ctx, testAggregate)
		require.NoError(t, err)

		aggregateStore := NewAggregateStore(inMemoryEventStore)

		// Create a separate event store for the materializer
		materializerEventStore := testMemoryStore(t)
		err = materializerEventStore.RegisterAggregate(ctx, testAggregate)
		require.NoError(t, err)

		// Ensure the topic exists in both event stores
		err = inMemoryEventStore.EnsureTopic(context.Background(), testAggregate.GetTopic(), testAggregate.GetPartitionCount())
		require.NoError(t, err)

		err = materializerEventStore.EnsureTopic(context.Background(), testAggregate.GetTopic(), testAggregate.GetPartitionCount())
		require.NoError(t, err)

		materializer := NewMaterializer(
			"test-subscriber",
			testAggregate.GetTopic(),
			[]int{0, 1},
			func(ctx context.Context, aggregateRoot AggregateRoot, version int) error {
				return nil
			},
			testAggregate,
			aggregateStore,
			true,
			WithMaterializerEventStore(materializerEventStore),
		)

		// We can only test that the materializer is created successfully
		assert.NotNil(t, materializer, "Materializer should not be nil")

		// Verify that the interface methods exist
		assert.NotPanics(t, func() {
			_ = materializer.Start()
			materializer.Stop()
		}, "Materializer should implement Start and Stop methods")
	})
}

func TestMaterializerStart(t *testing.T) {
	ctx := context.Background()
	// Use TestAggregate instead of mock
	testAggregate := NewTestAggregate()
	testAggregate.Id = "test-" + rand.Text()

	// Use a real AggregateStore with InMemoryEventStore
	inMemoryEventStore := testMemoryStore(t)
	err := inMemoryEventStore.RegisterAggregate(ctx, testAggregate)
	require.NoError(t, err)

	aggregateStore := NewAggregateStore(inMemoryEventStore)

	// Ensure the topic exists
	err = inMemoryEventStore.EnsureTopic(context.Background(), testAggregate.GetTopic(), testAggregate.GetPartitionCount())
	require.NoError(t, err)

	// Create a real StreamProcessor
	processor := NewStreamProcessor(
		inMemoryEventStore,
		"test-subscriber",
		testAggregate.GetTopic(),
		[]int{0, 1},
		true,
		func(envelope Envelope) error {
			return nil
			// No-op handler for testing
		},
	)

	m := &materializer{
		handler:         func(ctx context.Context, aggregateRoot AggregateRoot, version int) error { return nil },
		aggregateStore:  aggregateStore,
		aggregateType:   testAggregate,
		streamProcessor: processor,
	}

	resultChan := m.Start()

	assert.NotNil(t, resultChan, "Start should return an error channel")

	// Clean up
	m.Stop()
}

func TestMaterializerStop(t *testing.T) {
	ctx := context.Background()

	// Use TestAggregate instead of mock
	testAggregate := NewTestAggregate()
	testAggregate.Id = "test-" + rand.Text()

	// Use a real AggregateStore with InMemoryEventStore
	inMemoryEventStore := testMemoryStore(t)
	err := inMemoryEventStore.RegisterAggregate(ctx, testAggregate)
	require.NoError(t, err)

	aggregateStore := NewAggregateStore(inMemoryEventStore)

	// Ensure the topic exists
	err = inMemoryEventStore.EnsureTopic(context.Background(), testAggregate.GetTopic(), testAggregate.GetPartitionCount())
	require.NoError(t, err)

	// Create a real StreamProcessor
	processor := NewStreamProcessor(
		inMemoryEventStore,
		"test-subscriber",
		testAggregate.GetTopic(),
		[]int{0, 1},
		true,
		func(envelope Envelope) error {
			// No-op handler for testing
			return nil
		},
	)

	m := &materializer{
		handler:         func(ctx context.Context, aggregateRoot AggregateRoot, version int) error { return nil },
		aggregateStore:  aggregateStore,
		aggregateType:   testAggregate,
		streamProcessor: processor,
	}

	// This should not panic
	assert.NotPanics(t, func() {
		m.Stop()
	})
}

func TestMaterializerProcessEvent(t *testing.T) {
	ctx := context.Background()

	// Create a test event
	orgId := "test-org-id"
	aggregateId := "test-agg-" + rand.Text()

	// Use TestAggregate instead of mock
	testAggregate := NewTestAggregate()
	testAggregate.Id = aggregateId
	testAggregate.OrgId = orgId

	// Use a real AggregateStore with InMemoryEventStore
	inMemoryEventStore := testMemoryStore(t)
	err := inMemoryEventStore.RegisterAggregate(ctx, testAggregate)
	require.NoError(t, err)

	aggregateStore := NewAggregateStore(inMemoryEventStore)

	// Ensure the topic exists
	err = inMemoryEventStore.EnsureTopic(context.Background(), testAggregate.GetTopic(), testAggregate.GetPartitionCount())
	require.NoError(t, err)

	// Create and save an event for the aggregate to establish initial state
	event := &TestAggregateCreated{
		AggregateId: aggregateId,
		OrgId:       orgId,
		Name:        "Test Aggregate",
		Description: "Test Description",
		CreatedAt:   time.Now(),
		TimeStamp:   time.Now(),
		TopicName:   testAggregate.GetTopic(),
		Version:     1,
	}

	// Save the event to establish state
	streamName, err := testAggregate.GetStreamName(testAggregate)
	require.NoError(t, err)

	we := WriteEvents{
		Topic:        testAggregate.GetTopic(),
		StreamName:   streamName,
		PartitionKey: streamName,
		Events:       []Event{event},
		OrgId:        orgId,
	}

	err = inMemoryEventStore.SaveEvents(context.Background(), []WriteEvents{we})
	require.NoError(t, err)

	// Create the envelope for testing processEvent
	mockEvent := NewEnvelope(testAggregate.GetTopic(), orgId, streamName, time.Now(), 1, event, "")

	t.Run("SuccessfulProcessing", func(t *testing.T) {
		handlerCalled := false
		handler := func(ctx context.Context, aggregateRoot AggregateRoot, version int) error {
			handlerCalled = true
			assert.Equal(t, 1, version, "Version should be passed correctly to handler")
			return nil
		}

		m := &materializer{
			handler:        handler,
			aggregateStore: aggregateStore,
			aggregateType:  testAggregate,
		}

		errInner := m.processEvent(ctx, mockEvent)
		require.NoError(t, errInner)

		assert.True(t, handlerCalled, "Handler should be called")
	})

	t.Run("HandlerError", func(t *testing.T) {
		errorHandler := func(ctx context.Context, aggregateRoot AggregateRoot, version int) error {
			return errors.New("handler error")
		}

		m := &materializer{
			handler:        errorHandler,
			aggregateStore: aggregateStore,
			aggregateType:  testAggregate,
		}

		// This should not panic even though the handler returns an error
		assert.NotPanics(t, func() {
			errInner := m.processEvent(ctx, mockEvent)
			assert.Error(t, errInner)
		})
	})
}

package es

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Error constants for testing
var (
	ErrUnknownCommand        = errors.New("unknown command type")
	ErrInvalidAggregateState = errors.New("invalid aggregate state")
)

// TestAggregate implements the AggregateRoot interface for testing
type TestAggregate struct {
	AggregateBase
	Name          string
	Description   string
	CreatedAt     time.Time
	Activated     bool
	OverrideTopic string
}

// NewTestAggregate creates a new test aggregate
func NewTestAggregate() *TestAggregate {
	return &TestAggregate{
		AggregateBase: AggregateBase{},
	}
}

// GetAggregateType returns the type of the aggregate
func (a *TestAggregate) GetAggregateType() string {
	return "TestAggregate"
}

// GetTopic returns the topic for this aggregate type
func (a *TestAggregate) GetTopic() string {
	if a.OverrideTopic != "" {
		return a.OverrideTopic
	} else {
		return "test-topic"
	}
}

func (a *TestAggregate) GetPartitionCount() int {
	return 3
}

// InitialState returns a new instance of the aggregate
func (a *TestAggregate) InitialState() AggregateRoot {
	return NewTestAggregate()
}

// RegisterEvents registers event types for deserialization
func (a *TestAggregate) RegisterEvents(register func(Event)) {
	register(&TestAggregateCreated{})
	register(&TestAggregateActivated{})
}

// getBase returns the aggregate base instance
func (a *TestAggregate) getBase() *AggregateBase {
	return &a.AggregateBase
}

// HandleEvent processes an event and updates the aggregate's state
func (a *TestAggregate) HandleEvent(ctx context.Context, event Event, env *Envelope) error {
	switch evt := event.(type) {
	case *TestAggregateCreated:
		a.Id = evt.AggregateId
		a.OrgId = evt.OrgId
		a.Name = evt.Name
		a.Description = evt.Description
		a.CreatedAt = evt.CreatedAt
	case *TestAggregateActivated:
		a.Activated = true
	}

	return nil
}

// HandleCommand processes a command and returns resulting events
func (a *TestAggregate) HandleCommand(ctx context.Context, command interface{}) ([]Event, error) {
	switch cmd := command.(type) {
	case *CreateTestAggCommand:
		// Create a new TestAggregate event
		event := &TestAggregateCreated{
			AggregateId: cmd.AggregateId,
			OrgId:       cmd.OrgId,
			Name:        cmd.Name,
			Description: cmd.Description,
			CreatedAt:   time.Now(),
			TimeStamp:   time.Now(),
			TopicName:   "test-topic",
			Version:     1, // Set the version to 1
		}

		// Apply domain-specific logic to update the aggregate
		a.Id = event.AggregateId
		a.Name = event.Name
		a.Description = event.Description
		a.CreatedAt = event.CreatedAt

		// Use the standalone ApplyEvent function to track the event and update state
		if err := ApplyEvent(ctx, a, event); err != nil {
			return nil, err
		}

		return []Event{event}, nil
	case *ActivateCommand:
		// Create a new activation event
		event := &TestAggregateActivated{
			OrgId:       cmd.OrgId,
			AggregateId: cmd.AggregateId,
			TimeStamp:   time.Now(),
			TopicName:   "test-topic",
			Version:     2, // Set the version to 2
		}

		// Use the standalone ApplyEvent function to track the event and update state
		if err := ApplyEvent(ctx, a, event); err != nil {
			return nil, err
		}

		return []Event{event}, nil
	default:
		return nil, ErrUnknownCommand
	}
}

// CreateTestAggCommand is a mock command for testing
type CreateTestAggCommand struct {
	OrgId       string
	AggregateId string
	Name        string
	Description string
}

// ActivateCommand is a mock command for activating a test aggregate
type ActivateCommand struct {
	OrgId       string
	AggregateId string
}

// TestAggregateStore_Load tests loading an aggregate from the store
func TestAggregateStore_Load(t *testing.T) {
	ctx := context.Background()

	// Create in-memory event store
	eventStore := testMemoryStore(t)

	// Create aggregate store with the event store
	aggregateStore := NewAggregateStore(eventStore)

	// Create a test aggregate
	aggregateId := "test-aggregate-123"
	testAggregate := NewTestAggregate()
	testAggregate.Id = aggregateId

	// Create and save an event for the aggregate
	event := &TestAggregateCreated{
		AggregateId: aggregateId,
		OrgId:       "1",
		Name:        "Test Aggregate",
		Description: "A test aggregate",
		TimeStamp:   time.Now(),
		TopicName:   "test-topic",
		Version:     1, // Set the version to 1
	}

	streamName, err := GetStreamName(testAggregate, "1", aggregateId)
	require.NoError(t, err)

	we := WriteEvents{
		Topic:        "test-topic",
		OrgId:        event.OrgId,
		StreamName:   streamName,
		PartitionKey: streamName,
		Events:       []Event{event},
	}

	// Save the event to the store directly
	err = eventStore.SaveEvents(ctx, []WriteEvents{we})
	assert.NoError(t, err)

	// Load the aggregate
	err = aggregateStore.Load(ctx, "1", aggregateId, testAggregate)
	assert.NoError(t, err)

	// Verify the aggregate was loaded correctly
	assert.Equal(t, aggregateId, testAggregate.Id)
	assert.Equal(t, "Test Aggregate", testAggregate.Name)
	assert.Equal(t, "A test aggregate", testAggregate.Description)
	assert.False(t, testAggregate.Activated)
}

// TestAggregateStore_Save tests saving an aggregate to the store
func TestAggregateStore_Save(t *testing.T) {
	ctx := context.Background()

	// Create in-memory event store
	eventStore := testMemoryStore(t)

	// Create aggregate store with the event store
	aggregateStore := NewAggregateStore(eventStore)

	// Create a test aggregate
	aggregateId := "test-aggregate-456"
	testAggregate := NewTestAggregate()
	testAggregate.Id = aggregateId

	// Create a command
	command := &CreateTestAggCommand{
		AggregateId: aggregateId,
		OrgId:       "1",
		Name:        "Test Aggregate",
		Description: "A test aggregate for saving",
	}

	// Handle the command to generate events
	events, err := testAggregate.HandleCommand(ctx, command)
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	// Verify uncommitted events are tracked
	uncommittedEvents := testAggregate.GetUncommittedEvents()
	assert.Len(t, uncommittedEvents, 1, "Should have one uncommitted event")

	// Save the aggregate
	err = aggregateStore.Save(ctx, testAggregate)
	assert.NoError(t, err)

	// Verify uncommitted events are cleared after save
	assert.Empty(t, testAggregate.GetUncommittedEvents(), "Should have no uncommitted events after save")

	streamName, err := GetStreamName(testAggregate, "1", aggregateId)
	require.NoError(t, err)

	// Verify the events were saved to the store
	var limit *int
	savedEvents, err := eventStore.GetEvents(ctx, "test-topic", streamName, 0, limit)
	assert.NoError(t, err)
	assert.Len(t, savedEvents, 1, "Should have saved one event")

	// Verify the event details
	if assert.NotEmpty(t, savedEvents, "Should have at least one saved event") {
		savedEvent, ok := savedEvents[0].event.(*TestAggregateCreated)
		assert.True(t, ok, "Event should be a TestCreatedEvent")
		assert.Equal(t, "test-aggregate-created", savedEvent.GetEventType())
		assert.Equal(t, aggregateId, savedEvent.AggregateId)
		assert.Equal(t, "Test Aggregate", savedEvent.Name)
	}
}

// TestAggregateStore_FullLifecycle tests the full lifecycle of an aggregate
func TestAggregateStore_FullLifecycle(t *testing.T) {
	ctx := context.Background()

	// Create in-memory event store
	eventStore := testMemoryStore(t)

	// Create aggregate store with the event store
	aggregateStore := NewAggregateStore(eventStore)

	// Create a test aggregate
	aggregateId := "test-aggregate-789"
	testAggregate := NewTestAggregate()
	testAggregate.Id = aggregateId

	// 1. Handle command to create the aggregate
	createCommand := &CreateTestAggCommand{
		OrgId:       "1",
		AggregateId: aggregateId,
		Name:        "Lifecycle Test",
		Description: "Testing the full aggregate lifecycle",
	}

	events, err := testAggregate.HandleCommand(ctx, createCommand)
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	// Verify uncommitted events are cleared
	assert.NotEmpty(t, testAggregate.GetUncommittedEvents())

	// Verify the aggregate state has been immediately updated after handling the command
	assert.Equal(t, aggregateId, testAggregate.Id, "Aggregate ID should be set immediately after command is handled")
	assert.Equal(t, "Lifecycle Test", testAggregate.Name, "Aggregate Name should be updated immediately")
	assert.Equal(t, "Testing the full aggregate lifecycle", testAggregate.Description, "Aggregate Description should be updated immediately")
	assert.False(t, testAggregate.CreatedAt.IsZero(), "CreatedAt should be set immediately")
	assert.Equal(t, 1, testAggregate.GetVersion(), "Aggregate version should be 1 after first event")

	// 2. Save the aggregate
	err = aggregateStore.Save(ctx, testAggregate)
	assert.NoError(t, err)

	// Verify uncommitted events are cleared
	assert.Empty(t, testAggregate.GetUncommittedEvents())

	// 3. Create a new aggregate instance and load from the store
	loadedAggregate := NewTestAggregate()
	err = aggregateStore.Load(ctx, "1", aggregateId, loadedAggregate)
	assert.NoError(t, err)

	// 4. Verify the loaded aggregate has the correct state
	assert.Equal(t, aggregateId, loadedAggregate.Id)
	assert.Equal(t, "Lifecycle Test", loadedAggregate.Name)
	assert.Equal(t, "Testing the full aggregate lifecycle", loadedAggregate.Description)
	assert.False(t, loadedAggregate.Activated)

	// 5. Handle an "activate" command instead of directly applying an event
	activateCommand := &ActivateCommand{
		OrgId:       "1",
		AggregateId: aggregateId,
	}

	// Handle the command to generate activation event
	events, err = loadedAggregate.HandleCommand(ctx, activateCommand)
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	// 6. Save the updated aggregate
	err = aggregateStore.Save(ctx, loadedAggregate)
	assert.NoError(t, err)

	// 7. Load the aggregate again
	finalAggregate := NewTestAggregate()
	err = aggregateStore.Load(ctx, "1", aggregateId, finalAggregate)
	assert.NoError(t, err)

	// 8. Verify the final state
	assert.Equal(t, aggregateId, finalAggregate.Id)
	assert.Equal(t, "Lifecycle Test", finalAggregate.Name)
	assert.Equal(t, "Testing the full aggregate lifecycle", finalAggregate.Description)
	assert.True(t, finalAggregate.Activated)
}

// TestAggregateStore_Load_EmptyID tests that loading with an empty ID returns an error
func TestAggregateStore_Load_EmptyId(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create a test aggregate with an empty ID
	testAggregate := NewTestAggregate()

	// Should return error for empty ID
	err := aggregateStore.Load(ctx, "1", "", testAggregate)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "aggregate ID cannot be empty")
}

// TestAggregateStore_Load_AggregateNotFound tests that loading a non-existent aggregate returns ErrAggregateNotFound
func TestAggregateStore_Load_AggregateNotFound(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create a test aggregate with an ID that doesn't exist in the event store
	testAggregate := NewTestAggregate()
	testAggregate.Id = "non-existent-aggregate"

	// Should return ErrAggregateNotFound
	err := aggregateStore.Load(ctx, "1", "non-existent-aggregate", testAggregate)
	assert.Error(t, err)
	assert.Equal(t, ErrAggregateNotFound, err)
}

// MockFailingEventStore is an event store that always fails with a specific error
type MockFailingEventStore struct {
	LoadError   error
	SaveError   error
	savedEvents map[string][]Envelope // Add internal storage for events
}

func (m *MockFailingEventStore) GetTailCursor(context.Context, string, int) (string, error) {
	panic("unexpected tail operation in aggregate-store test")
}

func (m *MockFailingEventStore) GetSubscriptionCursor(ctx context.Context, topic, subscriberId string, partitionId int) (string, error) {
	panic("unexpected cursor operation in aggregate-store test")
}

func (m *MockFailingEventStore) SaveSubscriptionCursor(ctx context.Context, topic, subscriberId string, partitionId int, cursor string) error {
	panic("unexpected cursor operation in aggregate-store test")
}

var _ EventStore = (*MockFailingEventStore)(nil)

// NewMockFailingEventStore creates a new MockFailingEventStore
func NewMockFailingEventStore(loadError, saveError error) *MockFailingEventStore {
	return &MockFailingEventStore{
		LoadError:   loadError,
		SaveError:   saveError,
		savedEvents: make(map[string][]Envelope),
	}
}

func (m *MockFailingEventStore) GetEvents(ctx context.Context, topic string, streamName string, fromVersion int, limit *int) ([]Envelope, error) {
	if m.LoadError != nil {
		return nil, m.LoadError
	}

	// Return saved events if they exist
	key := topic + "/" + streamName
	if events, ok := m.savedEvents[key]; ok {
		return events, nil
	}

	return []Envelope{}, nil
}

func (m *MockFailingEventStore) SaveEvents(ctx context.Context, events []WriteEvents) error {
	if m.SaveError != nil {
		return m.SaveError
	}

	// Actually save the events when no error
	for _, writeEvents := range events {
		key := writeEvents.Topic + "/" + writeEvents.StreamName

		if writeEvents.ExpectedVersion != nil {
			latest, err := m.GetLatestEvent(ctx, writeEvents.Topic, writeEvents.StreamName)
			if err != nil {
				return fmt.Errorf("error getting latest event: %w", err)
			}

			if latest != nil {
				actualVersion := latest.GetVersion()
				if actualVersion != *writeEvents.ExpectedVersion {
					return &OptimisticConcurrencyErr{
						StreamName:      writeEvents.StreamName,
						ExpectedVersion: *writeEvents.ExpectedVersion,
						ActualVersion:   actualVersion,
					}
				}
			}
		}

		for _, event := range writeEvents.Events {
			latest, err := m.GetLatestEvent(ctx, writeEvents.Topic, writeEvents.StreamName)
			if err != nil {
				return fmt.Errorf("error getting latest event: %w", err)
			}

			version := 1
			if latest != nil {
				version = latest.GetVersion() + 1
			}

			// Create envelope and save
			env := NewEnvelope(writeEvents.Topic, writeEvents.OrgId, writeEvents.StreamName, time.Now(), version, event, writeEvents.CorrelationId)
			m.savedEvents[key] = append(m.savedEvents[key], env)
		}
	}

	return nil
}

func (m *MockFailingEventStore) GetEventsByTopic(ctx context.Context, topic string, partitionId int, cursor string) (EventsPage, error) {
	if m.LoadError != nil {
		return EventsPage{Events: nil, NextCursor: ""}, m.LoadError
	}

	// If no error, return empty events with no cursor
	return EventsPage{Events: []Envelope{}, NextCursor: ""}, nil
}

func (m *MockFailingEventStore) GetLatestEvent(ctx context.Context, topic string, streamName string) (*Envelope, error) {
	if m.LoadError != nil {
		return nil, m.LoadError
	}

	// If we have events, return the latest one
	key := topic + "/" + streamName
	if events, ok := m.savedEvents[key]; ok && len(events) > 0 {
		return &events[len(events)-1], nil
	}

	return nil, nil
}

func (m *MockFailingEventStore) GetEventsByOrganizations(ctx context.Context, topic string, partitionId int, orgIds []string, cursor string) (EventsPage, error) {
	if m.LoadError != nil {
		return EventsPage{Events: nil, NextCursor: ""}, m.LoadError
	}

	// If no error, return empty events with no cursor
	return EventsPage{Events: []Envelope{}, NextCursor: ""}, nil
}

func (m *MockFailingEventStore) GetEventsByOrgs(ctx context.Context, topic string, partitionId int, orgIds []string, cursor string) (EventsPage, error) {
	if m.LoadError != nil {
		return EventsPage{Events: nil, NextCursor: ""}, m.LoadError
	}

	// If no error, return empty events with no cursor
	return EventsPage{Events: []Envelope{}, NextCursor: ""}, nil
}

// EnsureTopic ensures a topic exists with the specified partition count
// For this mock implementation, it always returns the SaveError if set
func (m *MockFailingEventStore) EnsureTopic(ctx context.Context, topicName string, partitionCount int) error {
	if m.SaveError != nil {
		return m.SaveError
	}
	return nil
}

// RegisterAggregate registers an aggregate root with the event store
// For this mock implementation, it does nothing
func (m *MockFailingEventStore) RegisterAggregate(ctx context.Context, aggregate AggregateRoot) error {
	// No-op for testing
	return nil
}

// TestAggregateStore_Load_EventStoreError tests handling of event store errors during load
func TestAggregateStore_Load_EventStoreError(t *testing.T) {
	ctx := context.Background()
	expectedError := errors.New("event store load error")
	eventStore := NewMockFailingEventStore(expectedError, nil)
	aggregateStore := NewAggregateStore(eventStore)

	testAggregate := NewTestAggregate()
	testAggregate.Id = "test-aggregate"

	// Should return error from event store
	err := aggregateStore.Load(ctx, "1", "test-aggregate", testAggregate)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
}

// TestAggregateStore_Save_EmptyID tests that saving with an empty ID returns an error
func TestAggregateStore_Save_EmptyId(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create a test aggregate with an empty ID
	testAggregate := NewTestAggregate()

	// Should return error for empty ID
	err := aggregateStore.Save(ctx, testAggregate)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "aggregate ID cannot be empty")
}

// TestAggregateStore_Save_EmptyID tests that saving with an empty ID returns an error
func TestAggregateStore_Save_EmptyOrgId(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create a test aggregate with an empty ID
	testAggregate := NewTestAggregate()
	testAggregate.Id = "test-aggregate"

	// Should return error for empty ID
	err := aggregateStore.Save(ctx, testAggregate)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "aggregate OrgId cannot be empty")
}

// TestAggregateStore_Save_EventStoreError tests handling of event store errors during save
func TestAggregateStore_Save_EventStoreError(t *testing.T) {
	ctx := context.Background()
	expectedError := errors.New("event store save error")
	eventStore := NewMockFailingEventStore(nil, expectedError)
	aggregateStore := NewAggregateStore(eventStore)

	// Create test aggregate with an uncommitted event
	testAggregate := NewTestAggregate()
	testAggregate.Id = "test-aggregate"

	// Add uncommitted event by handling a command
	cmd := &CreateTestAggCommand{
		AggregateId: "test-aggregate",
		OrgId:       "1",
		Name:        "Test",
		Description: "Test Description",
	}
	_, err := testAggregate.HandleCommand(ctx, cmd)
	assert.NoError(t, err)

	// Should return error from event store
	err = aggregateStore.Save(ctx, testAggregate)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())

	// Verify that the aggregate can still be saved after a failed save attempt
	// This is the behavior we care about - resilience to transient errors
	eventStore.SaveError = nil // Clear the error for the next save attempt
	err = aggregateStore.Save(ctx, testAggregate)
	assert.NoError(t, err)

	streamName, err := GetStreamName(testAggregate, "1", "test-aggregate")
	require.NoError(t, err)

	// Verify the event was eventually saved
	var limit *int
	events, err := eventStore.GetEvents(ctx, "test-topic", streamName, 0, limit)
	assert.NoError(t, err)
	assert.NotEmpty(t, events, "Events should be saved after retry")
}

// TestAggregateStore_Save_NoEvents tests saving an aggregate with no uncommitted events
func TestAggregateStore_Save_NoEvents(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create a test aggregate with ID but no uncommitted events
	testAggregate := NewTestAggregate()
	testAggregate.Id = "test-aggregate-no-events"
	testAggregate.OrgId = "o1"

	// Should not return error and no events should be saved to store
	err := aggregateStore.Save(ctx, testAggregate)
	assert.NoError(t, err)

	// Verify no events were saved
	var limit *int
	events, err := eventStore.GetEvents(ctx, "test-topic", "test-aggregate-no-events", 0, limit)
	assert.NoError(t, err)
	assert.Empty(t, events)
}

// TestAggregateStore_Save_Idempotence tests that saving the same aggregate multiple times works correctly
// and doesn't duplicate events or corrupt the aggregate state
func TestAggregateStore_Save_Idempotence(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create a test aggregate
	aggregateId := "test-aggregate-idempotent"
	testAggregate := NewTestAggregate()
	testAggregate.Id = aggregateId

	// Create and handle a command to generate an event
	cmd := &CreateTestAggCommand{
		AggregateId: aggregateId,
		OrgId:       "1",
		Name:        "Idempotent Test",
		Description: "Testing idempotent saves",
	}
	events, err := testAggregate.HandleCommand(ctx, cmd)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, testAggregate.GetVersion(), 1)

	// First save - should succeed
	err = aggregateStore.Save(ctx, testAggregate)
	assert.NoError(t, err)

	streamName, err := GetStreamName(testAggregate, "1", aggregateId)
	require.NoError(t, err)

	// Get the number of events after first save
	var limit *int
	eventsAfterFirstSave, err := eventStore.GetEvents(ctx, "test-topic", streamName, 0, limit)
	assert.NoError(t, err)
	assert.Len(t, eventsAfterFirstSave, 1, "Should have exactly one event after first save")

	// Save again - should succeed with no errors
	err = aggregateStore.Save(ctx, testAggregate)
	assert.NoError(t, err)

	// Verify that a second save doesn't duplicate events
	eventsAfterSecondSave, err := eventStore.GetEvents(ctx, "test-topic", streamName, 0, limit)
	assert.NoError(t, err)
	assert.Len(t, eventsAfterSecondSave, 1, "Should still have exactly one event after second save")

	// Create and add a second event
	activateCmd := &ActivateCommand{
		AggregateId: aggregateId,
	}
	events, err = testAggregate.HandleCommand(ctx, activateCmd)
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	// Save the aggregate with the new event
	err = aggregateStore.Save(ctx, testAggregate)
	assert.NoError(t, err)

	// Verify we now have exactly two events
	eventsAfterThirdSave, err := eventStore.GetEvents(ctx, "test-topic", streamName, 0, limit)
	assert.NoError(t, err)
	assert.Len(t, eventsAfterThirdSave, 2, "Should have exactly two events after adding a second event")

	// Save again - should still be idempotent
	err = aggregateStore.Save(ctx, testAggregate)
	assert.NoError(t, err)

	// Verify that a fourth save still leaves us with exactly two events
	eventsAfterFourthSave, err := eventStore.GetEvents(ctx, "test-topic", streamName, 0, limit)
	assert.NoError(t, err)
	assert.Len(t, eventsAfterFourthSave, 2, "Should still have exactly two events after fourth save")

	// Verify that the events have correct sequence of versions
	assert.Equal(t, 1, eventsAfterFourthSave[0].GetVersion(), "First event should have version 1")
	assert.Equal(t, 2, eventsAfterFourthSave[1].GetVersion(), "Second event should have version 2")

	// Load the aggregate into a fresh instance
	freshAggregate := NewTestAggregate()
	err = aggregateStore.Load(ctx, "1", aggregateId, freshAggregate)
	assert.NoError(t, err)

	// Verify the freshly loaded aggregate has the correct state
	assert.Equal(t, aggregateId, freshAggregate.Id)
	assert.Equal(t, "Idempotent Test", freshAggregate.Name)
	assert.Equal(t, "Testing idempotent saves", freshAggregate.Description)
	assert.True(t, freshAggregate.Activated, "Aggregate should be activated")
}

// TestAggregateStore_SaveBatch tests saving multiple aggregates to the store
func TestAggregateStore_SaveBatch(t *testing.T) {
	ctx := context.Background()

	// Create in-memory event store
	eventStore := testMemoryStore(t)

	// Create aggregate store with the event store
	aggregateStore := NewAggregateStore(eventStore)

	// Create two test aggregates
	aggregateId1 := "test-aggregate-batch-1"
	testAggregate1 := NewTestAggregate()
	testAggregate1.Id = aggregateId1

	aggregateId2 := "test-aggregate-batch-2"
	testAggregate2 := NewTestAggregate()
	testAggregate2.Id = aggregateId2

	// Create commands
	command1 := &CreateTestAggCommand{
		AggregateId: aggregateId1,
		OrgId:       "1",
		Name:        "Test Aggregate 1",
		Description: "First test aggregate for batch saving",
	}

	command2 := &CreateTestAggCommand{
		AggregateId: aggregateId2,
		OrgId:       "1",
		Name:        "Test Aggregate 2",
		Description: "Second test aggregate for batch saving",
	}

	// Handle the commands to generate events
	events1, err := testAggregate1.HandleCommand(ctx, command1)
	assert.NoError(t, err)
	assert.Len(t, events1, 1)

	events2, err := testAggregate2.HandleCommand(ctx, command2)
	assert.NoError(t, err)
	assert.Len(t, events2, 1)

	// Verify uncommitted events are tracked
	uncommittedEvents1 := testAggregate1.GetUncommittedEvents()
	assert.Len(t, uncommittedEvents1, 1, "Should have one uncommitted event")

	uncommittedEvents2 := testAggregate2.GetUncommittedEvents()
	assert.Len(t, uncommittedEvents2, 1, "Should have one uncommitted event")

	// Save the aggregates in batch
	err = aggregateStore.SaveBatch(ctx, []AggregateRoot{testAggregate1, testAggregate2})
	assert.NoError(t, err)

	// Verify uncommitted events are cleared after save
	assert.Empty(t, testAggregate1.GetUncommittedEvents(), "Should have no uncommitted events after save")
	assert.Empty(t, testAggregate2.GetUncommittedEvents(), "Should have no uncommitted events after save")

	// Verify the events were saved to the store for the first aggregate
	streamName1, err := GetStreamName(testAggregate1, "1", aggregateId1)
	require.NoError(t, err)

	var limit *int
	savedEvents1, err := eventStore.GetEvents(ctx, "test-topic", streamName1, 0, limit)
	assert.NoError(t, err)
	assert.Len(t, savedEvents1, 1, "Should have saved one event for first aggregate")

	// Verify the events were saved to the store for the second aggregate
	streamName2, err := GetStreamName(testAggregate2, "1", aggregateId2)
	require.NoError(t, err)

	savedEvents2, err := eventStore.GetEvents(ctx, "test-topic", streamName2, 0, limit)
	assert.NoError(t, err)
	assert.Len(t, savedEvents2, 1, "Should have saved one event for second aggregate")
}

// TestAggregateStore_SaveBatch_EmptyId tests that saving with an empty ID returns an error
func TestAggregateStore_SaveBatch_EmptyId(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create two test aggregates, one with empty ID
	testAggregate1 := NewTestAggregate()
	testAggregate1.Id = "valid-id"
	testAggregate1.OrgId = "1"

	testAggregate2 := NewTestAggregate()
	// Empty ID for second aggregate
	testAggregate2.OrgId = "1"

	// Should return error for empty ID
	err := aggregateStore.SaveBatch(ctx, []AggregateRoot{testAggregate1, testAggregate2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "aggregate ID cannot be empty")
}

// TestAggregateStore_SaveBatch_EmptyOrgId tests that saving with an empty OrgId returns an error
func TestAggregateStore_SaveBatch_EmptyOrgId(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create two test aggregates, one with empty OrgId
	testAggregate1 := NewTestAggregate()
	testAggregate1.Id = "valid-id-1"
	testAggregate1.OrgId = "1"

	testAggregate2 := NewTestAggregate()
	testAggregate2.Id = "valid-id-2"
	// Empty OrgId for second aggregate

	// Should return error for empty OrgId
	err := aggregateStore.SaveBatch(ctx, []AggregateRoot{testAggregate1, testAggregate2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "aggregate OrgId cannot be empty")
}

// TestAggregateStore_SaveBatch_EventStoreError tests handling of event store errors during batch save
func TestAggregateStore_SaveBatch_EventStoreError(t *testing.T) {
	ctx := context.Background()
	expectedError := errors.New("event store save error")
	eventStore := NewMockFailingEventStore(nil, expectedError)
	aggregateStore := NewAggregateStore(eventStore)

	// Create test aggregates with uncommitted events
	testAggregate1 := NewTestAggregate()
	testAggregate1.Id = "test-aggregate-1"
	testAggregate1.OrgId = "1"

	testAggregate2 := NewTestAggregate()
	testAggregate2.Id = "test-aggregate-2"
	testAggregate2.OrgId = "1"

	// Add uncommitted events by handling commands
	cmd1 := &CreateTestAggCommand{
		AggregateId: "test-aggregate-1",
		OrgId:       "1",
		Name:        "Test 1",
		Description: "Test Description 1",
	}
	_, err := testAggregate1.HandleCommand(ctx, cmd1)
	assert.NoError(t, err)

	cmd2 := &CreateTestAggCommand{
		AggregateId: "test-aggregate-2",
		OrgId:       "1",
		Name:        "Test 2",
		Description: "Test Description 2",
	}
	_, err = testAggregate2.HandleCommand(ctx, cmd2)
	assert.NoError(t, err)

	// Should return error from event store
	err = aggregateStore.SaveBatch(ctx, []AggregateRoot{testAggregate1, testAggregate2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())

	// Verify that the aggregates can still be saved after a failed save attempt
	eventStore.SaveError = nil // Clear the error for the next save attempt
	err = aggregateStore.SaveBatch(ctx, []AggregateRoot{testAggregate1, testAggregate2})
	assert.NoError(t, err)

	// Verify the events were eventually saved for both aggregates
	var limit *int

	streamName1, err := GetStreamName(testAggregate1, "1", "test-aggregate-1")
	require.NoError(t, err)
	events1, err := eventStore.GetEvents(ctx, "test-topic", streamName1, 0, limit)
	assert.NoError(t, err)
	assert.NotEmpty(t, events1, "Events should be saved for first aggregate after retry")

	streamName2, err := GetStreamName(testAggregate2, "1", "test-aggregate-2")
	require.NoError(t, err)
	events2, err := eventStore.GetEvents(ctx, "test-topic", streamName2, 0, limit)
	assert.NoError(t, err)
	assert.NotEmpty(t, events2, "Events should be saved for second aggregate after retry")
}

// TestAggregateStore_SaveBatch_NoEvents tests saving aggregates with no events
func TestAggregateStore_SaveBatch_NoEvents(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create two test aggregates with no events
	testAggregate1 := NewTestAggregate()
	testAggregate1.Id = "test-aggregate-no-events-1"
	testAggregate1.OrgId = "1"

	testAggregate2 := NewTestAggregate()
	testAggregate2.Id = "test-aggregate-no-events-2"
	testAggregate2.OrgId = "1"

	// Should not error, but do nothing
	err := aggregateStore.SaveBatch(ctx, []AggregateRoot{testAggregate1, testAggregate2})
	assert.NoError(t, err)

	// Verify no events were saved for either aggregate
	var limit *int

	streamName1, err := GetStreamName(testAggregate1, "1", "test-aggregate-no-events-1")
	require.NoError(t, err)
	events1, err := eventStore.GetEvents(ctx, "test-topic", streamName1, 0, limit)
	assert.NoError(t, err)
	assert.Empty(t, events1, "No events should be saved for first aggregate")

	streamName2, err := GetStreamName(testAggregate2, "1", "test-aggregate-no-events-2")
	require.NoError(t, err)
	events2, err := eventStore.GetEvents(ctx, "test-topic", streamName2, 0, limit)
	assert.NoError(t, err)
	assert.Empty(t, events2, "No events should be saved for second aggregate")
}

// TestAggregateStore_SaveBatch_MixedEvents tests saving a batch with some aggregates having events and others not
func TestAggregateStore_SaveBatch_MixedEvents(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Create three test aggregates - two with events, one without
	testAggregate1 := NewTestAggregate()
	testAggregate1.Id = "test-aggregate-mixed-1"
	testAggregate1.OrgId = "1"

	testAggregate2 := NewTestAggregate()
	testAggregate2.Id = "test-aggregate-mixed-2"
	testAggregate2.OrgId = "1"

	testAggregate3 := NewTestAggregate()
	testAggregate3.Id = "test-aggregate-mixed-3"
	testAggregate3.OrgId = "1"

	cmd1 := &CreateTestAggCommand{
		AggregateId: "test-aggregate-mixed-1",
		OrgId:       "1",
		Name:        "Test 1",
		Description: "Test Description 1",
	}
	_, err := testAggregate1.HandleCommand(ctx, cmd1)
	assert.NoError(t, err)

	cmd3 := &CreateTestAggCommand{
		AggregateId: "test-aggregate-mixed-3",
		OrgId:       "1",
		Name:        "Test 3",
		Description: "Test Description 3",
	}
	_, err = testAggregate3.HandleCommand(ctx, cmd3)
	assert.NoError(t, err)

	// Save the batch
	err = aggregateStore.SaveBatch(ctx, []AggregateRoot{testAggregate1, testAggregate2, testAggregate3})
	assert.NoError(t, err)

	// Verify events were saved only for aggregates with events
	var limit *int

	streamName1, err := GetStreamName(testAggregate1, "1", "test-aggregate-mixed-1")
	require.NoError(t, err)
	events1, err := eventStore.GetEvents(ctx, "test-topic", streamName1, 0, limit)
	assert.NoError(t, err)
	assert.NotEmpty(t, events1, "Events should be saved for first aggregate")

	streamName2, err := GetStreamName(testAggregate2, "1", "test-aggregate-mixed-2")
	require.NoError(t, err)
	events2, err := eventStore.GetEvents(ctx, "test-topic", streamName2, 0, limit)
	assert.NoError(t, err)
	assert.Empty(t, events2, "No events should be saved for second aggregate")

	streamName3, err := GetStreamName(testAggregate3, "1", "test-aggregate-mixed-3")
	require.NoError(t, err)
	events3, err := eventStore.GetEvents(ctx, "test-topic", streamName3, 0, limit)
	assert.NoError(t, err)
	assert.NotEmpty(t, events3, "Events should be saved for third aggregate")
}

// TestAggregateStore_SaveBatch_EmptySlice tests saving an empty slice of aggregates
func TestAggregateStore_SaveBatch_EmptySlice(t *testing.T) {
	ctx := context.Background()
	eventStore := testMemoryStore(t)
	aggregateStore := NewAggregateStore(eventStore)

	// Should not error with empty slice
	err := aggregateStore.SaveBatch(ctx, []AggregateRoot{})
	assert.NoError(t, err)
}

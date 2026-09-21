package es

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockEvent implements the Event interface for testing
type MockEvent struct {
	Version     int       `json:"version"`
	EventType   string    `json:"eventType"`
	StreamName  string    `json:"streamName"`
	AggregateId string    `json:"aggregateId"`
	Topic       string    `json:"topic"`
	ShouldFail  bool      `json:"shouldFail"`
	Timestamp   time.Time `json:"timestamp"`
	OrgId       string    `json:"orgId"`
}

func (m *MockEvent) GetAggregateId() string {
	return m.AggregateId
}

func (e *MockEvent) GetEventType() string {
	return e.EventType
}

// Serialize implements Event interface
func (e *MockEvent) Serialize() ([]byte, error) {
	return json.Marshal(e)
}

// Deserialize implements Event interface
func (e *MockEvent) Deserialize(data []byte) error {
	return json.Unmarshal(data, e)
}

// GetEventCodec returns nil for testing purposes
func (e *MockEvent) GetEventCodec() EventCodec {
	return nil // For testing, we don't need a real codec implementation
}

// MockAggregate implements AggregateRoot for testing error paths
type MockAggregate struct {
	AggregateBase
	shouldFailHandleEvent bool
	aggregateType         string
	topic                 string
	// Add fields to track state changes
	processedEventTypes []string
	name                string
	description         string
	activated           bool
}

func NewMockAggregate(id string, shouldFailHandleEvent bool) *MockAggregate {
	a := &MockAggregate{
		shouldFailHandleEvent: shouldFailHandleEvent,
		aggregateType:         "mock-aggregate",
		topic:                 "test-topic",
		processedEventTypes:   make([]string, 0),
	}
	a.Id = id
	return a
}

var _ AggregateRoot = (*MockAggregate)(nil)

func (a *MockAggregate) HandleEvent(ctx context.Context, event Event, env *Envelope) error {
	if a.shouldFailHandleEvent {
		return errors.New("intentional handle event failure")
	}

	// Check if the specific event should fail
	if mockEvent, ok := event.(*MockEvent); ok && mockEvent.ShouldFail {
		return errors.New("event-specific failure")
	}

	// Track the event type to verify ordering
	a.processedEventTypes = append(a.processedEventTypes, event.GetEventType())

	// Update state based on event type
	if mockEvent, ok := event.(*MockEvent); ok {
		// Set some state values based on the event
		switch mockEvent.EventType {
		case "test1":
			a.name = "Event 1 processed"
		case "test2":
			a.description = "Event 2 processed"
		case "test3":
			a.activated = true
		}
	}

	return nil
}

func (a *MockAggregate) InitialState() AggregateRoot {
	// Return a fresh instance
	return NewMockAggregate(a.Id, a.shouldFailHandleEvent)
}

func (a *MockAggregate) GetStreamName(root AggregateRoot) (string, error) {
	return "mock-stream-" + a.Id, nil
}

func (a *MockAggregate) GetAggregateType() string {
	return a.aggregateType
}

func (a *MockAggregate) GetTopic() string {
	return a.topic
}

func (a *MockAggregate) GetPartitionCount() int {
	return 1
}

func (a *MockAggregate) HandleCommand(ctx context.Context, command interface{}) ([]Event, error) {
	// Not needed for these tests
	return nil, nil
}

func (a *MockAggregate) RegisterEvents(r func(Event)) {
	// Register mock event type
	r(&MockEvent{})
}

// TestLoadHistoricEvents_EmptyEvents tests loading an empty slice of events
func TestLoadHistoricEvents_EmptyEvents(t *testing.T) {
	ctx := context.Background()
	aggregate := NewMockAggregate("test", false)

	err := aggregate.LoadHistoricEvents(ctx, []Envelope{}, aggregate)
	assert.NoError(t, err, "Loading empty events should not produce an error")
}

// TestLoadHistoricEvents_FirstEventNotVersion1 tests error handling when the first event doesn't have version 1
func TestLoadHistoricEvents_FirstEventNotVersion1(t *testing.T) {
	ctx := context.Background()
	aggregate := NewMockAggregate("test", false)

	mockEvent := &MockEvent{
		Version:    2, // Should be 1
		EventType:  "test",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	events := []Envelope{
		NewEnvelope(
			mockEvent.Topic,
			mockEvent.OrgId,
			mockEvent.StreamName,
			time.Now(),
			mockEvent.Version,
			mockEvent,
			"",
		),
	}

	err := aggregate.LoadHistoricEvents(ctx, events, aggregate)

	// Verify the error is correct
	require.Error(t, err, "Should return an error when first event version is not 1")

	// Verify it's the right error type
	var eventHandlingError *EventHandlingError
	assert.True(t, errors.As(err, &eventHandlingError), "Error should be an EventHandlingError")

	// Verify error message
	assert.Contains(t, err.Error(), "not the next contiguous version 1", "Error message should explain the problem")

	// Verify the aggregate state didn't change
	assert.Equal(t, 0, aggregate.Version, "Aggregate version should not change")
	assert.Empty(t, aggregate.processedEventTypes, "No events should be processed")
}

// TestLoadHistoricEvents_VersionConflict tests error handling when there's a version conflict
func TestLoadHistoricEvents_VersionConflict(t *testing.T) {
	ctx := context.Background()
	aggregate := NewMockAggregate("test", false)
	aggregate.Version = 2 // Pre-set a version higher than the incoming event

	mockEvent := &MockEvent{
		Version:    1,
		EventType:  "test",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	events := []Envelope{
		NewEnvelope(
			mockEvent.Topic,
			mockEvent.OrgId,
			mockEvent.StreamName,
			time.Now(),
			mockEvent.Version,
			mockEvent,
			"",
		),
	}

	err := aggregate.LoadHistoricEvents(ctx, events, aggregate)

	// Verify the error is correct
	require.Error(t, err, "Should return an error when aggregate version conflicts with event version")

	// Verify it's the right error type
	var eventHandlingError *EventHandlingError
	assert.True(t, errors.As(err, &eventHandlingError), "Error should be an EventHandlingError")

	// Verify error message
	assert.Contains(t, err.Error(), "not the next contiguous version 3", "Error message should explain the problem")

	// Verify the aggregate state didn't change
	assert.Equal(t, 2, aggregate.Version, "Aggregate version should not change")
	assert.Empty(t, aggregate.processedEventTypes, "No events should be processed")
}

// TestLoadHistoricEvents_HandleEventError tests error handling when HandleEvent returns an error
func TestLoadHistoricEvents_HandleEventError(t *testing.T) {
	ctx := context.Background()
	aggregate := NewMockAggregate("test", true) // Set to fail HandleEvent

	mockEvent := &MockEvent{
		Version:    1,
		EventType:  "test",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	events := []Envelope{
		NewEnvelope(
			mockEvent.Topic,
			mockEvent.OrgId,
			mockEvent.StreamName,
			time.Now(),
			mockEvent.Version,
			mockEvent,
			"",
		),
	}

	err := aggregate.LoadHistoricEvents(ctx, events, aggregate)

	// Verify the error is correct
	require.Error(t, err, "Should return an error when HandleEvent fails")

	// Verify it's the right error type
	var eventHandlingError *EventHandlingError
	assert.True(t, errors.As(err, &eventHandlingError), "Error should be an EventHandlingError")

	// Verify error message
	assert.Contains(t, err.Error(), "intentional handle event failure", "Error message should include the original error")

	// Verify the aggregate state didn't change
	assert.Equal(t, 0, aggregate.Version, "Aggregate version should not change")
	assert.Empty(t, aggregate.processedEventTypes, "No events should be processed")
}

// TestLoadHistoricEvents_SpecificEventFailure tests handling of an event that specifically fails
func TestLoadHistoricEvents_SpecificEventFailure(t *testing.T) {
	ctx := context.Background()
	aggregate := NewMockAggregate("test", false)

	mockEvent1 := &MockEvent{
		Version:    1,
		EventType:  "test1",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		ShouldFail: false, // First event succeeds
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	mockEvent2 := &MockEvent{
		Version:    2,
		EventType:  "test2",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		ShouldFail: true, // Second event fails
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	events := []Envelope{
		NewEnvelope(
			mockEvent1.Topic,
			mockEvent1.OrgId,
			mockEvent1.StreamName,
			time.Now(),
			mockEvent1.Version,
			mockEvent1,
			"",
		),
		NewEnvelope(
			mockEvent2.Topic,
			mockEvent2.OrgId,
			mockEvent2.StreamName,
			time.Now(),
			mockEvent2.Version,
			mockEvent2,
			"",
		),
	}

	err := aggregate.LoadHistoricEvents(ctx, events, aggregate)

	// Verify the error is correct
	require.Error(t, err, "Should return an error when an event fails")

	// Verify it's the right error type
	var eventHandlingError *EventHandlingError
	assert.True(t, errors.As(err, &eventHandlingError), "Error should be an EventHandlingError")

	// Verify error message
	assert.Contains(t, err.Error(), "event-specific failure", "Error message should include the original error")

	// Verify the aggregate state reflects only the first event
	assert.Equal(t, 1, aggregate.Version, "Aggregate version should match the last successful event")
	assert.Len(t, aggregate.processedEventTypes, 1, "Only the successful event should be processed")
}

// TestLoadHistoricEvents_Success tests successful loading of events
func TestLoadHistoricEvents_Success(t *testing.T) {
	ctx := context.Background()
	aggregate := NewMockAggregate("test", false)

	mockEvent1 := &MockEvent{
		Version:    1,
		EventType:  "test1",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	mockEvent2 := &MockEvent{
		Version:    2,
		EventType:  "test2",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	mockEvent3 := &MockEvent{
		Version:    3,
		EventType:  "test3",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	events := []Envelope{
		NewEnvelope(
			mockEvent1.Topic,
			mockEvent1.OrgId,
			mockEvent1.StreamName,
			time.Now(),
			mockEvent1.Version,
			mockEvent1,
			"",
		),
		NewEnvelope(
			mockEvent2.Topic,
			mockEvent2.OrgId,
			mockEvent2.StreamName,
			time.Now(),
			mockEvent2.Version,
			mockEvent2,
			"",
		),
		NewEnvelope(
			mockEvent3.Topic,
			mockEvent3.OrgId,
			mockEvent3.StreamName,
			time.Now(),
			mockEvent3.Version,
			mockEvent3,
			"",
		),
	}

	err := aggregate.LoadHistoricEvents(ctx, events, aggregate)
	assert.NoError(t, err, "Loading valid events should not produce an error")

	// Verify the aggregate state
	assert.Equal(t, 3, aggregate.Version, "Aggregate version should match the last event")
	assert.Len(t, aggregate.processedEventTypes, 3, "All events should be processed")
}

// TestLoadHistoricEvents_EventsNotSorted tests that events are properly sorted by version
func TestLoadHistoricEvents_EventsNotSorted(t *testing.T) {
	ctx := context.Background()
	aggregate := NewMockAggregate("test", false)

	mockEvent1 := &MockEvent{
		Version:    1,
		EventType:  "test1",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	mockEvent2 := &MockEvent{
		Version:    2,
		EventType:  "test2",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	mockEvent3 := &MockEvent{
		Version:    3,
		EventType:  "test3",
		StreamName: "mock-stream-test",
		Topic:      "test-topic",
		Timestamp:  time.Now(),
		OrgId:      "test-org",
	}

	// Events in wrong order
	events := []Envelope{
		NewEnvelope(
			mockEvent3.Topic,
			mockEvent3.OrgId,
			mockEvent3.StreamName,
			time.Now(),
			mockEvent3.Version,
			mockEvent3,
			"",
		),
		NewEnvelope(
			mockEvent1.Topic,
			mockEvent1.OrgId,
			mockEvent1.StreamName,
			time.Now(),
			mockEvent1.Version,
			mockEvent1,
			"",
		),
		NewEnvelope(
			mockEvent2.Topic,
			mockEvent2.OrgId,
			mockEvent2.StreamName,
			time.Now(),
			mockEvent2.Version,
			mockEvent2,
			"",
		),
	}

	err := aggregate.LoadHistoricEvents(ctx, events, aggregate)
	assert.NoError(t, err, "Loading events in wrong order should be handled by sorting")

	// Verify the aggregate state
	assert.Equal(t, 3, aggregate.Version, "Aggregate version should match the last event by version")

	// Verify the state was updated in the correct order by checking the final state
	assert.Equal(t, "Event 1 processed", aggregate.name, "Name should be set by the first event")
	assert.Equal(t, "Event 2 processed", aggregate.description, "Description should be set by the second event")
	assert.True(t, aggregate.activated, "Activated should be set to true by the third event")

	// Check that events were processed in the correct order
	assert.Equal(t, []string{"test1", "test2", "test3"}, aggregate.processedEventTypes,
		"Events should be processed in version order regardless of input order")
}

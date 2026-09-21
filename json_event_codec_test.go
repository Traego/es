package es

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Define a mock EventBase type for testing
type TestEventBase struct {
	OrgId         string    `json:"orgId"`
	AggregateId   string    `json:"aggregateId"`
	AggregateType string    `json:"aggregateType"`
	EventType     string    `json:"eventType"`
	Version       int       `json:"version"`
	Timestamp     time.Time `json:"timestamp"`
}

func (e *TestEventBase) GetAggregateId() string {
	return e.AggregateId
}

func (e *TestEventBase) GetEventType() string {
	return e.EventType
}

func (e *TestEventBase) GetEventCodec() EventCodec {
	return &JsonEventCodec{}
}

// Define data portion of test event
type TestEventData struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	UserCount   int       `json:"userCount"`
	CreatedAt   time.Time `json:"createdAt"`
}

// Define a composite event using both types
type TestCompositeEvent struct {
	TestEventBase
	TestEventData
}

func Test_JsonEventCodec_SerializeDeserialize(t *testing.T) {
	// Arrange
	now := time.Now().UTC()

	// Create a composite event
	originalEvent := &TestCompositeEvent{
		TestEventBase: TestEventBase{
			OrgId:         "test-org-123",
			AggregateId:   "aggregate-456",
			AggregateType: "test-type",
			EventType:     "test-event-created",
			Version:       1,
			Timestamp:     now,
		},
		TestEventData: TestEventData{
			Name:        "Test Event",
			Description: "A test event for codec serialization",
			UserCount:   42,
			CreatedAt:   now,
		},
	}

	// Create the codec
	codec := JsonEventCodec{}

	// Set up a context
	ctx := context.Background()

	// Act - Serialize
	serialized, err := codec.Serialize(ctx, originalEvent)
	require.NoError(t, err)
	assert.NotEmpty(t, serialized)

	// Act - Deserialize
	deserializedEvent := &TestCompositeEvent{}
	err = codec.Deserialize(ctx, serialized, deserializedEvent)
	require.NoError(t, err)

	// Assert - Base fields
	assert.Equal(t, originalEvent.OrgId, deserializedEvent.OrgId)
	assert.Equal(t, originalEvent.AggregateId, deserializedEvent.AggregateId)
	assert.Equal(t, originalEvent.AggregateType, deserializedEvent.AggregateType)
	assert.Equal(t, originalEvent.EventType, deserializedEvent.EventType)
	assert.Equal(t, originalEvent.Version, deserializedEvent.Version)
	assert.Equal(t, originalEvent.Timestamp.Unix(), deserializedEvent.Timestamp.Unix())

	// Assert - Data fields
	assert.Equal(t, originalEvent.Name, deserializedEvent.Name)
	assert.Equal(t, originalEvent.Description, deserializedEvent.Description)
	assert.Equal(t, originalEvent.UserCount, deserializedEvent.UserCount)
	assert.Equal(t, originalEvent.CreatedAt.Unix(), deserializedEvent.CreatedAt.Unix())
}

// Add a test with nested structures to ensure the codec correctly handles more complex event types
type NestedData struct {
	Values    []string          `json:"values"`
	Metadata  map[string]string `json:"metadata"`
	IsEnabled bool              `json:"isEnabled"`
}

type ComplexEventData struct {
	Title      string     `json:"title"`
	Nested     NestedData `json:"nested"`
	Properties []string   `json:"properties,omitempty"`
}

type ComplexEvent struct {
	TestEventBase
	ComplexEventData
}

func Test_JsonEventCodec_ComplexEvent(t *testing.T) {
	// Arrange
	now := time.Now().UTC()

	// Create a complex event with nested structures
	originalEvent := &ComplexEvent{
		TestEventBase: TestEventBase{
			OrgId:         "test-org-789",
			AggregateId:   "aggregate-complex",
			AggregateType: "complex-type",
			EventType:     "complex-event",
			Version:       3,
			Timestamp:     now,
		},
		ComplexEventData: ComplexEventData{
			Title: "Complex Event",
			Nested: NestedData{
				Values:    []string{"value1", "value2", "value3"},
				Metadata:  map[string]string{"key1": "value1", "key2": "value2"},
				IsEnabled: true,
			},
			Properties: []string{"prop1", "prop2"},
		},
	}

	// Create the codec
	codec := JsonEventCodec{}

	// Set up a context
	ctx := context.Background()

	// Act - Serialize
	serialized, err := codec.Serialize(ctx, originalEvent)
	require.NoError(t, err)
	assert.NotEmpty(t, serialized)

	// Act - Deserialize
	deserializedEvent := &ComplexEvent{}
	err = codec.Deserialize(ctx, serialized, deserializedEvent)
	require.NoError(t, err)

	// Assert - Base fields
	assert.Equal(t, originalEvent.OrgId, deserializedEvent.OrgId)
	assert.Equal(t, originalEvent.AggregateType, deserializedEvent.AggregateType)
	assert.Equal(t, originalEvent.EventType, deserializedEvent.EventType)

	// Assert - Complex data fields
	assert.Equal(t, originalEvent.Title, deserializedEvent.Title)
	assert.Equal(t, originalEvent.Nested.Values, deserializedEvent.Nested.Values)
	assert.Equal(t, originalEvent.Nested.Metadata, deserializedEvent.Nested.Metadata)
	assert.Equal(t, originalEvent.Nested.IsEnabled, deserializedEvent.Nested.IsEnabled)
	assert.Equal(t, originalEvent.Properties, deserializedEvent.Properties)
}

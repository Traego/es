package es

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEventRegistry tests the event registry functionality
func TestEventRegistry(t *testing.T) {
	ctx := context.Background()

	// Create a new event registry
	registry := NewEventRegistry()
	require.NotNil(t, registry, "Registry should not be nil")

	// Create a test aggregate
	testAggregate := NewTestAggregate()
	require.NotNil(t, testAggregate, "Test aggregate should not be nil")

	// Register the aggregate with the registry
	err := registry.RegisterAggregate(ctx, testAggregate)
	require.NoError(t, err)

	// Create a test event
	testEvent := &TestAggregateCreated{
		OrgId:       "test-org",
		AggregateId: "test-123",
		Name:        "Test Event",
		Description: "Test event description",
		CreatedAt:   time.Now(),
		TimeStamp:   time.Now(),
		TopicName:   "test-topic",
		Version:     1,
	}

	// Get the prototype for the event type
	prototype, err := registry.GetEventPrototype(testEvent.GetEventType())
	assert.NoError(t, err, "Should find the registered event type")
	assert.NotNil(t, prototype, "Prototype should not be nil")
	assert.Equal(t, "test-aggregate-created", prototype.GetEventType(), "Event type should match")

	// Test that we can get aggregates for a topic
	aggregateTypes := registry.GetAggregatesForTopic("test-topic")
	assert.Len(t, aggregateTypes, 1, "Should have one aggregate type for the topic")
	assert.Equal(t, "TestAggregate", aggregateTypes[0], "Aggregate type should match")

	// Test that unknown event types return an error
	_, err = registry.GetEventPrototype("UnknownEventType")
	assert.Error(t, err, "Should return an error for unknown event types")
}

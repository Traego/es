package es

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockErrorEvent implements the Event interface for error testing
type MockErrorEvent struct {
	eventType string
	name      string
}

func (e *MockErrorEvent) GetAggregateId() string {
	return ""
}

func NewMockErrorEvent() *MockErrorEvent {
	return &MockErrorEvent{
		eventType: "test-event",
		name:      "error-test",
	}
}

func (e *MockErrorEvent) GetEventType() string {
	return e.eventType
}

func (e *MockErrorEvent) GetEventCodec() EventCodec {
	return JsonEventCodec{}
}

// TestEventHandlingError tests the EventHandlingError
func TestEventHandlingError(t *testing.T) {
	t.Run("CreateEventHandlingError", func(t *testing.T) {
		event := NewMockErrorEvent()
		originalErr := fmt.Errorf("original error")

		env := Envelope{
			event: event,
		}
		// Test creation using constructor
		err := NewEventHandlingError(env, originalErr)

		// Verify all fields were set correctly
		assert.Equal(t, event.GetEventType(), err.EventType)
		assert.Equal(t, originalErr, err.Err)
	})

	t.Run("ErrorMessage", func(t *testing.T) {
		event := NewMockErrorEvent()
		originalErr := fmt.Errorf("original error")

		env := Envelope{
			event: event,
		}

		err := NewEventHandlingError(env, originalErr)

		// Test error message format
		expectedMsg := fmt.Sprintf(
			"failed to handle event: type=%s org_id=%s version=%d: %v",
			event.GetEventType(),
			env.GetOrgId(), env.GetVersion(), originalErr,
		)
		assert.Equal(t, expectedMsg, err.Error())
	})

	t.Run("Unwrap", func(t *testing.T) {
		originalErr := fmt.Errorf("original error")
		env := Envelope{
			event: NewMockErrorEvent(),
		}
		err := NewEventHandlingError(env, originalErr)

		// Test unwrapping
		unwrappedErr := errors.Unwrap(err)
		assert.Equal(t, originalErr, unwrappedErr)

		// Test errors.Is functionality
		assert.True(t, errors.Is(err, originalErr))
	})

	t.Run("As", func(t *testing.T) {
		env := Envelope{
			event: NewMockErrorEvent(),
		}
		err := NewEventHandlingError(env, fmt.Errorf("test error"))

		var eventErr *EventHandlingError
		assert.True(t, errors.As(err, &eventErr))
		assert.Equal(t, err, eventErr)
	})
}

// TestEventSaveError tests the EventSaveError
func TestEventSaveError(t *testing.T) {
	t.Run("CreateEventSaveError", func(t *testing.T) {
		aggregateId := "test-aggregate"
		eventCount := 5
		originalErr := fmt.Errorf("original save error")

		// Test creation using constructor
		err := NewEventSaveError(aggregateId, eventCount, originalErr)

		// Verify all fields were set correctly
		assert.Equal(t, aggregateId, err.AggregateId)
		assert.Equal(t, eventCount, err.EventCount)
		assert.Equal(t, originalErr, err.Err)
	})

	t.Run("ErrorMessage", func(t *testing.T) {
		aggregateId := "test-aggregate"
		eventCount := 5
		originalErr := fmt.Errorf("original save error")
		err := NewEventSaveError(aggregateId, eventCount, originalErr)

		// Test error message format
		expectedMsg := fmt.Sprintf(
			"failed to save events: aggregate_id=%s event_count=%d: %v",
			aggregateId, eventCount, originalErr,
		)
		assert.Equal(t, expectedMsg, err.Error())
	})

	t.Run("Unwrap", func(t *testing.T) {
		originalErr := fmt.Errorf("original save error")
		err := NewEventSaveError("test-aggregate", 5, originalErr)

		// Test unwrapping
		unwrappedErr := errors.Unwrap(err)
		assert.Equal(t, originalErr, unwrappedErr)

		// Test errors.Is functionality
		assert.True(t, errors.Is(err, originalErr))
	})

	t.Run("As", func(t *testing.T) {
		err := NewEventSaveError("test-aggregate", 5, fmt.Errorf("test error"))

		var saveErr *EventSaveError
		assert.True(t, errors.As(err, &saveErr))
		assert.Equal(t, err, saveErr)
	})
}

// TestEventLoadError tests the EventLoadError
func TestEventLoadError(t *testing.T) {
	t.Run("CreateEventLoadError", func(t *testing.T) {
		aggregateId := "test-aggregate"
		topic := "test-topic"
		originalErr := fmt.Errorf("original load error")

		// Test creation using constructor
		err := NewEventLoadError(aggregateId, topic, originalErr)

		// Verify all fields were set correctly
		assert.Equal(t, aggregateId, err.AggregateId)
		assert.Equal(t, topic, err.Topic)
		assert.Equal(t, originalErr, err.Err)
	})

	t.Run("ErrorMessage", func(t *testing.T) {
		aggregateId := "test-aggregate"
		topic := "test-topic"
		originalErr := fmt.Errorf("original load error")
		err := NewEventLoadError(aggregateId, topic, originalErr)

		// Test error message format
		expectedMsg := fmt.Sprintf(
			"failed to load events: aggregate_id=%s topic=%s: %v",
			aggregateId, topic, originalErr,
		)
		assert.Equal(t, expectedMsg, err.Error())
	})

	t.Run("Unwrap", func(t *testing.T) {
		originalErr := fmt.Errorf("original load error")
		err := NewEventLoadError("test-aggregate", "test-topic", originalErr)

		// Test unwrapping
		unwrappedErr := errors.Unwrap(err)
		assert.Equal(t, originalErr, unwrappedErr)

		// Test errors.Is functionality
		assert.True(t, errors.Is(err, originalErr))
	})

	t.Run("As", func(t *testing.T) {
		err := NewEventLoadError("test-aggregate", "test-topic", fmt.Errorf("test error"))

		var loadErr *EventLoadError
		assert.True(t, errors.As(err, &loadErr))
		assert.Equal(t, err, loadErr)
	})
}

// TestCommonErrors tests the common error variables defined in the package
func TestCommonErrors(t *testing.T) {
	t.Run("ErrAggregateNotFound", func(t *testing.T) {
		// Ensure the error is defined and has the expected message
		require.NotNil(t, ErrAggregateNotFound)
		assert.Equal(t, "aggregate not found", ErrAggregateNotFound.Error())

		// Test with wrapped errors
		wrappedErr := fmt.Errorf("wrapped: %w", ErrAggregateNotFound)
		assert.True(t, errors.Is(wrappedErr, ErrAggregateNotFound))
	})

	t.Run("WrapErrorsInDomainErrors", func(t *testing.T) {
		// Test wrapping common errors in our domain-specific error types
		notFoundErr := ErrAggregateNotFound

		// Wrap in EventLoadError
		loadErr := NewEventLoadError("test-agg", "test-topic", notFoundErr)
		assert.True(t, errors.Is(loadErr, ErrAggregateNotFound))

		// Wrap in EventSaveError
		saveErr := NewEventSaveError("test-agg", 0, notFoundErr)
		assert.True(t, errors.Is(saveErr, ErrAggregateNotFound))

		// Double wrapping
		env := Envelope{
			event: NewMockErrorEvent(),
		}
		doubleWrapErr := NewEventHandlingError(env, saveErr)
		assert.True(t, errors.Is(doubleWrapErr, saveErr))
		assert.True(t, errors.Is(doubleWrapErr, ErrAggregateNotFound))
	})
}

// TestErrorMultipleWrapping tests more complex error wrapping scenarios
func TestErrorMultipleWrapping(t *testing.T) {
	// Create a chain of errors
	rootErr := fmt.Errorf("root error")
	saveErr := NewEventSaveError("test-agg", 2, rootErr)
	env := Envelope{
		event: NewMockErrorEvent(),
	}
	handlingErr := NewEventHandlingError(env, saveErr)

	// Test direct unwrapping
	assert.Equal(t, saveErr, errors.Unwrap(handlingErr))
	assert.Equal(t, rootErr, errors.Unwrap(saveErr))

	// Test with errors.Is
	assert.True(t, errors.Is(handlingErr, saveErr))
	assert.True(t, errors.Is(handlingErr, rootErr))
	assert.True(t, errors.Is(saveErr, rootErr))

	// Test with errors.As
	var extractedSaveErr *EventSaveError
	assert.True(t, errors.As(handlingErr, &extractedSaveErr))
	assert.Equal(t, saveErr, extractedSaveErr)
}

// TestErrorWithContextualData tests that error context is preserved and accessible
func TestErrorWithContextualData(t *testing.T) {
	// Create an error with rich context
	event := NewMockErrorEvent()
	event.eventType = "special-event"
	event.name = "42"

	rootErr := fmt.Errorf("detailed error")
	env := Envelope{
		event:   event,
		orgId:   "important-org",
		version: 42,
	}
	err := NewEventHandlingError(env, rootErr)

	// Verify that we can extract the context even after wrapping
	wrappedErr := fmt.Errorf("something went wrong: %w", err)

	var extractedErr *EventHandlingError
	assert.True(t, errors.As(wrappedErr, &extractedErr))

	// Verify all contextual data is preserved
	assert.Equal(t, "special-event", extractedErr.EventType)
	assert.Equal(t, 42, extractedErr.Version)
	assert.Equal(t, "important-org", extractedErr.OrgId)
	assert.Equal(t, rootErr, extractedErr.Unwrap())
}

package es

import (
	"errors"
	"fmt"
)

// Common errors that can be returned by the aggregate store
var (
	ErrAggregateNotFound      = errors.New("aggregate not found")
	ErrOptimisticConcurrency  = errors.New("optimistic concurrency control violation: expected version does not match")
	ErrAggregateAlreadyExists = errors.New("aggregate already exists")
)

// EventHandlingError represents an error that occurred while handling an event
type EventHandlingError struct {
	EventType string
	OrgId     string
	Version   int
	Err       error
}

// Error implements the error interface
func (e *EventHandlingError) Error() string {
	return fmt.Sprintf("failed to handle event: type=%s org_id=%s version=%d: %v",
		e.EventType, e.OrgId, e.Version, e.Err)
}

// Unwrap returns the underlying error
func (e *EventHandlingError) Unwrap() error {
	return e.Err
}

// NewEventHandlingError creates a new EventHandlingError
func NewEventHandlingError(event Envelope, err error) *EventHandlingError {
	return &EventHandlingError{
		EventType: event.GetEventType(),
		OrgId:     event.GetOrgId(),
		Version:   event.GetVersion(),
		Err:       err,
	}
}

// EventSaveError represents an error that occurred while saving events
type EventSaveError struct {
	AggregateId string
	EventCount  int
	Err         error
}

// NewEventSaveError creates a new EventSaveError
func NewEventSaveError(aggregateId string, eventCount int, err error) *EventSaveError {
	return &EventSaveError{
		AggregateId: aggregateId,
		EventCount:  eventCount,
		Err:         err,
	}
}

// Error implements the error interface
func (e *EventSaveError) Error() string {
	return fmt.Sprintf("failed to save events: aggregate_id=%s event_count=%d: %v",
		e.AggregateId, e.EventCount, e.Err)
}

// Unwrap returns the underlying error
func (e *EventSaveError) Unwrap() error {
	return e.Err
}

// EventLoadError represents an error that occurred while loading events
type EventLoadError struct {
	AggregateId string
	Topic       string
	Err         error
}

// Error implements the error interface
func (e *EventLoadError) Error() string {
	return fmt.Sprintf("failed to load events: aggregate_id=%s topic=%s: %v",
		e.AggregateId, e.Topic, e.Err)
}

// Unwrap returns the underlying error
func (e *EventLoadError) Unwrap() error {
	return e.Err
}

// NewEventLoadError creates a new EventLoadError
func NewEventLoadError(aggregateId string, topic string, err error) *EventLoadError {
	return &EventLoadError{
		AggregateId: aggregateId,
		Topic:       topic,
		Err:         err,
	}
}

type OptimisticConcurrencyErr struct {
	StreamName      string
	ExpectedVersion int
	ActualVersion   int
}

// Error implements the error interface
func (e *OptimisticConcurrencyErr) Error() string {
	return fmt.Sprintf("optimistic concurrency control violation: stream=%s expected_version=%d actual_version=%d",
		e.StreamName, e.ExpectedVersion, e.ActualVersion)
}

// Unwrap allows errors.Is(err, ErrOptimisticConcurrency) through save wrappers.
func (e *OptimisticConcurrencyErr) Unwrap() error { return ErrOptimisticConcurrency }

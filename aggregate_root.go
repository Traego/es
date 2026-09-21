package es

import (
	"context"
	"errors"
	"fmt"
	"net/url"
)

// AggregateBaseLifecycle defines the infrastructure methods for aggregate event sourcing
type AggregateBaseLifecycle interface {
	GetVersion() int
	LoadHistoricEvents(ctx context.Context, events []Envelope, aggregateRoot AggregateRoot) error
	getBase() *AggregateBase
}

// AggregateRoot represents an aggregate root in domain-driven design.
// This interface focuses purely on domain logic concerns.
type AggregateRoot interface {
	AggregateBaseLifecycle

	// GetAggregateType returns the type of the aggregate
	GetAggregateType() string

	// GetId returns the ID of the aggregate
	GetId() string

	GetOrgId() string

	GetStreamName(root AggregateRoot) (string, error)

	// GetTopic returns the topic for this aggregate type
	GetTopic() string

	// GetPartitionCount returns the target partition count for the root - should scale with expected event volume
	GetPartitionCount() int

	// InitialState returns a fresh instance of the aggregate root
	InitialState() AggregateRoot

	// HandleCommand processes a command and returns resulting events
	// This method should contain the core domain logic and validation
	HandleCommand(ctx context.Context, command interface{}) (events []Event, err error)

	// HandleEvent processes an event on the current state
	HandleEvent(ctx context.Context, event Event, envelope *Envelope) error

	// RegisterEvents registers event types for deserialization
	RegisterEvents(r func(Event))
}

// GetStreamNameFromFullAgg computes the aggregate stream identity.
func GetStreamNameFromFullAgg(a AggregateRoot) (string, error) {
	if isNil(a) {
		return "", errors.New("aggregate is required")
	}
	orgId := a.GetOrgId()
	id := a.GetId()
	return GetStreamName(a, orgId, id)
}

// GetStreamName encodes aggregate type, organization, and identifier unambiguously.
func GetStreamName(a AggregateRoot, orgId, id string) (string, error) {
	if isNil(a) {
		return "", errors.New("aggregate is required")
	}
	if a.GetAggregateType() == "" {
		return "", errors.New("aggregate type not set")
	}

	if id == "" {
		return "", errors.New("aggregate ID cannot be empty")
	}
	if orgId == "" {
		return "", errors.New("aggregate OrgId cannot be empty")
	}

	return fmt.Sprintf(
		"%s/%s/%s",
		url.QueryEscape(a.GetAggregateType()),
		url.QueryEscape(orgId),
		url.QueryEscape(id),
	), nil
}

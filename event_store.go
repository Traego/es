package es

import (
	"context"
	"errors"
)

type EventsPage struct {
	Events     []Envelope
	NextCursor string
}

type WriteEvents struct {
	Topic           string
	OrgId           string
	StreamName      string
	PartitionKey    string
	ExpectedVersion *int
	Events          []Event
	CorrelationId   string
}

var (
	ErrCursorNotFound = errors.New("cursor not found")
)

type EventStore interface {
	SaveEvents(ctx context.Context, events []WriteEvents) error
	GetEvents(ctx context.Context, topic string, streamName string, fromVersion int, limit *int) ([]Envelope, error)
	GetEventsByTopic(ctx context.Context, topic string, partitionId int, cursor string) (EventsPage, error)
	GetEventsByOrgs(ctx context.Context, topic string, partitionId int, orgIds []string, cursor string) (EventsPage, error)
	GetLatestEvent(ctx context.Context, topic string, streamName string) (*Envelope, error)
	EnsureTopic(ctx context.Context, topicName string, partitionCount int) error
	RegisterAggregate(ctx context.Context, aggregate AggregateRoot) error
	GetSubscriptionCursor(ctx context.Context, topic, subscriberId string, partitionId int) (string, error)
	SaveSubscriptionCursor(ctx context.Context, topic, subscriberId string, partitionId int, cursor string) error
}

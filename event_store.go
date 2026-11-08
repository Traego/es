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

// EventStore persists events and subscription checkpoints. Durable adapters keep
// checkpoints in a table keyed by topic, subscriber ID and partition. Event rows
// are unique by topic, stream name and version; batch appends and version checks
// are atomic. Cursor contents are private to the backend.
//
// GetSubscriptionCursor returns ErrCursorNotFound only when no checkpoint exists.
// A saved empty cursor is an explicit beginning-of-history checkpoint. Stores do
// not decide how a new subscriber starts; the subscriber supplies that policy.
type EventStore interface {
	// GetTailCursor captures the current end of one topic partition without
	// consuming history. Events appended after this boundary remain readable.
	// An empty partition still yields a valid starting boundary, possibly "".
	GetTailCursor(ctx context.Context, topic string, partitionId int) (string, error)
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

package es

import (
	"context"
)

type EventCodec interface {
	Serialize(ctx context.Context, event Event) ([]byte, error)
	Deserialize(ctx context.Context, data []byte, event Event) error
}

type Event interface {
	GetEventType() string
	GetEventCodec() EventCodec
	GetAggregateId() string
}

type OrgEvent interface {
	Event
	GetOrgId() string
}

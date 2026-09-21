package es

import "time"

// TestAggregateCreated is a mock event for testing
type TestAggregateCreated struct {
	OrgId       string    `json:"orgId"`
	AggregateId string    `json:"aggregateId"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
	TimeStamp   time.Time `json:"timeStamp"`
	TopicName   string    `json:"topicName"`
	Version     int       `json:"version"`
}

func (e *TestAggregateCreated) GetAggregateId() string {
	return e.AggregateId
}

// GetEventType returns the type of the event
func (e *TestAggregateCreated) GetEventType() string {
	return "test-aggregate-created"
}

// GetTimestamp returns the timestamp of the event
func (e *TestAggregateCreated) GetTimestamp() time.Time {
	if e.TimeStamp.IsZero() {
		return time.Now()
	}
	return e.TimeStamp
}

// GetVersion returns the version of the event
func (e *TestAggregateCreated) GetVersion() int {
	return e.Version // Return the explicit version field
}

// GetOrgId returns the org ID associated with this event
func (e *TestAggregateCreated) GetOrgId() string {
	return e.OrgId
}

// GetEventCodec returns the event codec for this event
func (e *TestAggregateCreated) GetEventCodec() EventCodec {
	return JsonEventCodec{}
}

// TestCreatedEvent is a mock event for testing
type TestAggregateActivated struct {
	OrgId       string
	AggregateId string
	Name        string
	Description string
	CreatedAt   time.Time
	TimeStamp   time.Time
	TopicName   string
	Version     int // Add explicit version field
}

func (e *TestAggregateActivated) GetAggregateId() string {
	return e.AggregateId
}

// GetEventType returns the type of the event
func (e *TestAggregateActivated) GetEventType() string {
	return "test-aggregate-activated-event"
}

// GetVersion returns the version of the event
func (e *TestAggregateActivated) GetVersion() int {
	return e.Version // Return the explicit version field
}

// GetOrgId returns the org ID associated with this event
func (e *TestAggregateActivated) GetOrgId() string {
	return e.OrgId
}

// GetEventCodec returns the event codec for this event
func (e *TestAggregateActivated) GetEventCodec() EventCodec {
	return JsonEventCodec{} // For testing purposes, we don't need a real codec implementation
}

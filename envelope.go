package es

import "time"

type Envelope struct {
	topic          string
	orgId          string
	streamName     string
	globalPosition time.Time
	version        int
	event          Event
	correlationId  string
}

func NewEnvelope(topic, orgId, streamName string, globalPosition time.Time, version int, event Event, correlationId string) Envelope {
	return Envelope{
		topic:          topic,
		orgId:          orgId,
		streamName:     streamName,
		globalPosition: globalPosition,
		version:        version,
		event:          event,
		correlationId:  correlationId,
	}
}

func (e Envelope) GetTopic() string {
	return e.topic
}

func (e Envelope) GetOrgId() string {
	return e.orgId
}

func (e Envelope) GetStreamName() string {
	return e.streamName
}

func (e Envelope) GetGlobalPosition() time.Time {
	return e.globalPosition
}

func (e Envelope) GetVersion() int {
	return e.version
}

func (e Envelope) GetEvent() Event {
	return e.event
}

func (e Envelope) GetEventType() string {
	return e.event.GetEventType()
}

func (e Envelope) GetCorrelationId() string {
	return e.correlationId
}

package es

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
)

type registeredEvent struct {
	typ   reflect.Type
	codec EventCodec
}

// EventRegistry associates stable event names with Go types and codecs.
// Register aggregates before starting consumers. Registration is atomic, and
// repeating the same event name and Go type is idempotent. Codecs must be safe
// for concurrent use and must not mutate their input bytes or shared state.
type EventRegistry struct {
	mu           sync.RWMutex
	eventTypes   map[string]registeredEvent
	topicMapping map[string][]string
}

// NewEventRegistry constructs an empty registry.
func NewEventRegistry() *EventRegistry {
	return &EventRegistry{eventTypes: make(map[string]registeredEvent), topicMapping: make(map[string][]string)}
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// RegisterAggregate registers an aggregate's event prototypes. Prototypes must
// be non-nil pointers, with nonempty stable names and non-nil codecs.
func (r *EventRegistry) RegisterAggregate(ctx context.Context, aggregate AggregateRoot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNil(aggregate) {
		return errors.New("aggregate is required")
	}
	aggregateType, topic := aggregate.GetAggregateType(), aggregate.GetTopic()
	if aggregateType == "" || topic == "" {
		return errors.New("aggregate type and topic are required")
	}
	pending := make(map[string]registeredEvent)
	var validationErr error
	aggregate.RegisterEvents(func(event Event) {
		if isNil(event) || reflect.TypeOf(event).Kind() != reflect.Pointer {
			validationErr = errors.New("event prototype must be a non-nil pointer")
			return
		}
		name, codec := event.GetEventType(), event.GetEventCodec()
		if name == "" || isNil(codec) {
			validationErr = errors.New("event type and codec are required")
			return
		}
		entry := registeredEvent{typ: reflect.TypeOf(event), codec: codec}
		if old, ok := pending[name]; ok && old.typ != entry.typ {
			validationErr = fmt.Errorf("event type %q is registered with conflicting Go types", name)
			return
		}
		pending[name] = entry
	})
	if validationErr != nil {
		return validationErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, entry := range pending {
		if old, ok := r.eventTypes[name]; ok && old.typ != entry.typ {
			return fmt.Errorf("event type %q is already registered with a different Go type", name)
		}
	}
	for name, entry := range pending {
		r.eventTypes[name] = entry
	}
	if !slices.Contains(r.topicMapping[topic], aggregateType) {
		r.topicMapping[topic] = append(r.topicMapping[topic], aggregateType)
	}
	return nil
}

func (r *EventRegistry) lookup(eventType string) (registeredEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.eventTypes[eventType]
	if !ok {
		return registeredEvent{}, fmt.Errorf("unknown event type: %s", eventType)
	}
	return entry, nil
}

// GetEventPrototype returns a fresh zero-valued instance of a registered type.
func (r *EventRegistry) GetEventPrototype(eventType string) (Event, error) {
	entry, err := r.lookup(eventType)
	if err != nil {
		return nil, err
	}
	return reflect.New(entry.typ.Elem()).Interface().(Event), nil
}

// DeserializeEvent decodes into a new event using its registered codec.
func (r *EventRegistry) DeserializeEvent(ctx context.Context, eventType string, data []byte) (Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entry, err := r.lookup(eventType)
	if err != nil {
		return nil, err
	}
	event := reflect.New(entry.typ.Elem()).Interface().(Event)
	if err := entry.codec.Deserialize(ctx, data, event); err != nil {
		return nil, fmt.Errorf("failed to deserialize event: %w", err)
	}
	return event, nil
}

// GetAggregatesForTopic returns an independent copy of registered type names.
func (r *EventRegistry) GetAggregatesForTopic(topic string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string{}, r.topicMapping[topic]...)
}

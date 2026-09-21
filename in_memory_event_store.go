package es

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type memoryStream struct{ topic, name string }
type memoryStreamState struct {
	version, partition int
	org                string
}
type memorySubscription struct {
	topic, subscriber string
	partition         int
}

// memoryEvent owns its encoded data. Neither writes nor reads retain an event
// supplied by the caller. Codecs must support concurrent calls, as for any store.
type memoryEvent struct {
	topic, org, stream, correlation string
	version, partition              int
	inserted                        time.Time
	data                            []byte
	eventType                       reflect.Type
	codec                           EventCodec
}

// InMemoryEventStore is a concurrency-safe, process-local EventStore for tests
// and development. Use NewInMemoryEventStore and ensure topics before writing.
// Its opaque cursors are specific to this backend and must not be inspected.
type InMemoryEventStore struct {
	mu                  sync.RWMutex
	events              []memoryEvent
	streams             map[memoryStream]memoryStreamState
	pageSize            int
	topicPartitions     map[string]int
	eventRegistry       *EventRegistry
	subscriptionCursors map[memorySubscription]string
}

func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{
		streams:             make(map[memoryStream]memoryStreamState),
		pageSize:            50,
		topicPartitions:     make(map[string]int),
		eventRegistry:       NewEventRegistry(),
		subscriptionCursors: make(map[memorySubscription]string),
	}
}

// WithPageSize configures pagination. It panics if pageSize is not positive.
func (s *InMemoryEventStore) WithPageSize(pageSize int) *InMemoryEventStore {
	if pageSize <= 0 {
		panic("page size must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageSize = pageSize
	return s
}

// SaveEvents atomically appends a batch. Serialization is completed before the
// lock is acquired, and every expected version is checked before any commit.
// The first saved event fixes the stream's tenant and computed partition.
func (s *InMemoryEventStore) SaveEvents(ctx context.Context, writes []WriteEvents) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	seen := make(map[memoryStream]bool, len(writes))
	staged := make([][]memoryEvent, len(writes))
	for i, write := range writes {
		if write.Topic == "" || write.StreamName == "" || write.OrgId == "" {
			return fmt.Errorf("topic, stream name, and org ID must be nonempty")
		}
		key := memoryStream{write.Topic, write.StreamName}
		if seen[key] {
			return fmt.Errorf("duplicate topic/stream combination: %q/%q", write.Topic, write.StreamName)
		}
		seen[key] = true
		if write.ExpectedVersion != nil && *write.ExpectedVersion < 0 {
			return fmt.Errorf("expected version must not be negative")
		}
		for _, event := range write.Events {
			value := reflect.ValueOf(event)
			if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || event.GetEventType() == "" {
				return fmt.Errorf("event must be a nonnil pointer with a nonempty event type")
			}
			if orgEvent, ok := event.(OrgEvent); ok && orgEvent.GetOrgId() != "" && orgEvent.GetOrgId() != write.OrgId {
				return fmt.Errorf("event org ID does not match write org ID")
			}
			codec := event.GetEventCodec()
			if isNil(codec) {
				return fmt.Errorf("event codec must not be nil")
			}
			data, err := codec.Serialize(ctx, event)
			if err != nil {
				return fmt.Errorf("serialize event: %w", err)
			}
			staged[i] = append(staged[i], memoryEvent{
				topic: write.Topic, org: write.OrgId, stream: write.StreamName,
				correlation: write.CorrelationId, data: append([]byte(nil), data...),
				eventType: value.Type(), codec: codec,
			})
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	for i, write := range writes {
		count, exists := s.topicPartitions[write.Topic]
		if !exists {
			return fmt.Errorf("topic %q has not been ensured", write.Topic)
		}
		key := memoryStream{write.Topic, write.StreamName}
		state := s.streams[key]
		version := state.version
		if write.ExpectedVersion != nil && version != *write.ExpectedVersion {
			return &OptimisticConcurrencyErr{StreamName: write.StreamName, ExpectedVersion: *write.ExpectedVersion, ActualVersion: version}
		}
		partitionKey := write.PartitionKey
		if partitionKey == "" {
			partitionKey = write.StreamName
		}
		// Both inputs have been validated: a nonempty key and an ensured count.
		partition, _ := CalculatePartitionId(partitionKey, count)
		if state.version > 0 && (state.org != write.OrgId || state.partition != partition) {
			return fmt.Errorf("stream tenant and partition cannot change")
		}
		for j := range staged[i] {
			staged[i][j].partition = partition
			staged[i][j].version = version + j + 1
		}
	}
	for _, records := range staged {
		if len(records) == 0 {
			continue
		}
		for j := range records {
			records[j].inserted = time.Now().UTC()
		}
		s.events = append(s.events, records...)
		last := records[len(records)-1]
		s.streams[memoryStream{last.topic, last.stream}] = memoryStreamState{version: last.version, partition: last.partition, org: last.org}
	}
	return nil
}

// memoryEnvelopes decodes a complete result or returns no events on failure.
// The copy of the payload protects stored bytes from destructive codecs.
func memoryEnvelopes(ctx context.Context, records []memoryEvent) ([]Envelope, error) {
	var events []Envelope
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		event := reflect.New(record.eventType.Elem()).Interface().(Event)
		if err := record.codec.Deserialize(ctx, append([]byte(nil), record.data...), event); err != nil {
			return nil, fmt.Errorf("deserialize event: %w", err)
		}
		events = append(events, NewEnvelope(record.topic, record.org, record.stream, record.inserted, record.version, event, record.correlation))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// GetEvents returns stream versions in ascending order, fromVersion inclusive.
func (s *InMemoryEventStore) GetEvents(ctx context.Context, topic, streamName string, fromVersion int, limit *int) ([]Envelope, error) {
	if topic == "" || streamName == "" || fromVersion < 0 || (limit != nil && *limit <= 0) {
		return nil, fmt.Errorf("invalid stream query")
	}
	s.mu.RLock()
	var records []memoryEvent
	for _, record := range s.events {
		if record.topic == topic && record.stream == streamName && record.version >= fromVersion {
			records = append(records, record)
			if limit != nil && len(records) == *limit {
				break
			}
		}
	}
	s.mu.RUnlock()
	return memoryEnvelopes(ctx, records)
}

// GetLatestEvent returns nil when the stream has no events.
func (s *InMemoryEventStore) GetLatestEvent(ctx context.Context, topic, streamName string) (*Envelope, error) {
	if topic == "" || streamName == "" {
		return nil, fmt.Errorf("topic and stream name must be nonempty")
	}
	s.mu.RLock()
	var records []memoryEvent
	for i := len(s.events) - 1; i >= 0; i-- {
		if s.events[i].topic == topic && s.events[i].stream == streamName {
			records = append(records, s.events[i])
			break
		}
	}
	s.mu.RUnlock()
	events, err := memoryEnvelopes(ctx, records)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}
	return &events[0], nil
}

type memoryCursor struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Position  int    `json:"position"`
	Format    int    `json:"format"`
}

func (s *InMemoryEventStore) GetEventsByTopic(ctx context.Context, topic string, partitionId int, cursor string) (EventsPage, error) {
	if partitionId < 0 {
		return EventsPage{}, fmt.Errorf("partition must not be negative")
	}
	return s.GetEventsByOrgs(ctx, topic, partitionId, nil, cursor)
}

// GetEventsByOrgs reads an append-ordered page. Empty orgIds means all orgs;
// partitionId -1 means all partitions. Cursors remain valid when the org filter
// changes and resume strictly after their position, including at end of stream.
func (s *InMemoryEventStore) GetEventsByOrgs(ctx context.Context, topic string, partitionId int, orgIds []string, cursor string) (EventsPage, error) {
	if err := ctx.Err(); err != nil {
		return EventsPage{}, err
	}
	position := 0
	if cursor != "" {
		data, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return EventsPage{}, fmt.Errorf("invalid cursor: %w", ErrCursorNotFound)
		}
		var decoded memoryCursor
		if json.Unmarshal(data, &decoded) != nil || decoded.Format != 1 || decoded.Topic != topic || decoded.Partition != partitionId || decoded.Position < 0 {
			return EventsPage{}, fmt.Errorf("invalid cursor scope or format: %w", ErrCursorNotFound)
		}
		position = decoded.Position
	}
	s.mu.RLock()
	count, exists := s.topicPartitions[topic]
	if !exists || partitionId < -1 || partitionId >= count {
		s.mu.RUnlock()
		return EventsPage{}, fmt.Errorf("unknown topic or invalid partition")
	}
	if position > len(s.events) {
		s.mu.RUnlock()
		return EventsPage{}, fmt.Errorf("cursor position is beyond the store: %w", ErrCursorNotFound)
	}
	orgs := make(map[string]bool, len(orgIds))
	for _, org := range orgIds {
		orgs[org] = true
	}
	var records []memoryEvent
	for i := position; i < len(s.events); i++ {
		record := s.events[i]
		if record.topic != topic || (partitionId != -1 && record.partition != partitionId) || (len(orgs) > 0 && !orgs[record.org]) {
			continue
		}
		records = append(records, record)
		position = i + 1
		if len(records) == s.pageSize {
			break
		}
	}
	s.mu.RUnlock()
	events, err := memoryEnvelopes(ctx, records)
	if err != nil {
		return EventsPage{}, err
	}
	if len(records) > 0 {
		// The fixed string/integer shape cannot fail JSON encoding.
		data, _ := json.Marshal(memoryCursor{topic, partitionId, position, 1})
		cursor = base64.RawURLEncoding.EncodeToString(data)
	}
	return EventsPage{Events: events, NextCursor: cursor}, nil
}

// EnsureTopic is idempotent for an unchanged partition count. Changing a count
// would strand existing cursors and is rejected, including for empty topics.
func (s *InMemoryEventStore) EnsureTopic(ctx context.Context, topicName string, partitionCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if topicName == "" || partitionCount <= 0 || uint64(partitionCount) > 1<<32-1 {
		return fmt.Errorf("topic name must be nonempty and partition count between 1 and 4294967295")
	}
	if count, exists := s.topicPartitions[topicName]; exists && count != partitionCount {
		return fmt.Errorf("topic partition count cannot change from %d to %d", count, partitionCount)
	}
	s.topicPartitions[topicName] = partitionCount
	return nil
}

func (s *InMemoryEventStore) RegisterAggregate(ctx context.Context, aggregate AggregateRoot) error {
	if isNil(aggregate) {
		return fmt.Errorf("aggregate must not be nil")
	}
	if err := s.EnsureTopic(ctx, aggregate.GetTopic(), aggregate.GetPartitionCount()); err != nil {
		return err
	}
	return s.eventRegistry.RegisterAggregate(ctx, aggregate)
}

func (s *InMemoryEventStore) GetSubscriptionCursor(ctx context.Context, topic, subscriberId string, partitionId int) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if topic == "" || subscriberId == "" || partitionId < 0 {
		return "", fmt.Errorf("invalid subscription identity")
	}
	return s.subscriptionCursors[memorySubscription{topic, subscriberId, partitionId}], nil
}

func (s *InMemoryEventStore) SaveSubscriptionCursor(ctx context.Context, topic, subscriberId string, partitionId int, cursor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if topic == "" || subscriberId == "" || partitionId < 0 {
		return fmt.Errorf("invalid subscription identity")
	}
	s.subscriptionCursors[memorySubscription{topic, subscriberId, partitionId}] = cursor
	return nil
}

var _ EventStore = (*InMemoryEventStore)(nil)

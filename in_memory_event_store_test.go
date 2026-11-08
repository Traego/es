package es

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type memoryTestEvent struct {
	Kind    string              `json:"kind"`
	ID      string              `json:"id"`
	Org     string              `json:"org"`
	Payload map[string][]string `json:"payload"`
	Codec   EventCodec          `json:"-"`
	NoCodec bool                `json:"-"`
}

func (e *memoryTestEvent) GetEventType() string   { return e.Kind }
func (e *memoryTestEvent) GetAggregateId() string { return e.ID }
func (e *memoryTestEvent) GetOrgId() string       { return e.Org }
func (e *memoryTestEvent) GetEventCodec() EventCodec {
	if e.NoCodec {
		return nil
	}
	if e.Codec != nil {
		return e.Codec
	}
	return JsonEventCodec{}
}

func memoryEventFor(id string) *memoryTestEvent {
	return &memoryTestEvent{Kind: "memory-event", ID: id, Payload: map[string][]string{"values": {"original"}}}
}

type memoryValueEvent struct{}

func (memoryValueEvent) GetEventType() string      { return "value" }
func (memoryValueEvent) GetAggregateId() string    { return "value" }
func (memoryValueEvent) GetEventCodec() EventCodec { return JsonEventCodec{} }

type memoryTestCodec struct {
	serialize   func(context.Context, Event) ([]byte, error)
	deserialize func(context.Context, []byte, Event) error
}

// A named function can implement EventCodec; a typed nil must be rejected
// before its Serialize method would call the nil function and panic.
type memoryFunctionCodec func(context.Context, Event) ([]byte, error)

func (c memoryFunctionCodec) Serialize(ctx context.Context, event Event) ([]byte, error) {
	return c(ctx, event)
}
func (memoryFunctionCodec) Deserialize(_ context.Context, data []byte, event Event) error {
	return json.Unmarshal(data, event)
}

func (c *memoryTestCodec) Serialize(ctx context.Context, e Event) ([]byte, error) {
	if c.serialize != nil {
		return c.serialize(ctx, e)
	}
	return json.Marshal(e)
}
func (c *memoryTestCodec) Deserialize(ctx context.Context, data []byte, e Event) error {
	if c.deserialize != nil {
		return c.deserialize(ctx, data, e)
	}
	return json.Unmarshal(data, e)
}

type memoryTestAggregate struct {
	AggregateRoot
	topic      string
	partitions int
	prototype  Event
}

func (a *memoryTestAggregate) GetTopic() string                    { return a.topic }
func (a *memoryTestAggregate) GetPartitionCount() int              { return a.partitions }
func (a *memoryTestAggregate) GetAggregateType() string            { return "memory-test" }
func (a *memoryTestAggregate) RegisterEvents(register func(Event)) { register(a.prototype) }

func memoryStoreFor(t *testing.T, topics ...string) *InMemoryEventStore {
	t.Helper()
	s := NewInMemoryEventStore()
	for _, topic := range topics {
		require.NoError(t, s.EnsureTopic(context.Background(), topic, 1))
	}
	return s
}

func memoryWrite(topic, stream, org string, events ...Event) WriteEvents {
	return WriteEvents{Topic: topic, StreamName: stream, OrgId: org, Events: events}
}

func memoryCursorFor(topic string, partition, position int) string {
	data, _ := json.Marshal(memoryCursor{Topic: topic, Partition: partition, Position: position, Format: 1})
	return base64.RawURLEncoding.EncodeToString(data)
}

func TestMemoryImmutablePayloadAndMetadata(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "topic")
	event := memoryEventFor("id")
	write := memoryWrite("topic", "stream", "envelope-org", event)
	write.CorrelationId = "correlation"
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{write}))
	event.Payload["values"][0] = "caller mutation"
	event.ID = "changed"
	read, err := s.GetEvents(ctx, "topic", "stream", 0, nil)
	require.NoError(t, err)
	require.Len(t, read, 1)
	require.Equal(t, "original", read[0].GetEvent().(*memoryTestEvent).Payload["values"][0])
	require.Equal(t, "id", read[0].GetEvent().GetAggregateId())
	require.Equal(t, "envelope-org", read[0].GetOrgId())
	require.Empty(t, read[0].GetEvent().(*memoryTestEvent).Org)
	require.Equal(t, "topic", read[0].GetTopic())
	require.Equal(t, "stream", read[0].GetStreamName())
	require.Equal(t, "correlation", read[0].GetCorrelationId())
	require.Equal(t, 1, read[0].GetVersion())
	require.False(t, read[0].GetGlobalPosition().IsZero())
	read[0].GetEvent().(*memoryTestEvent).Payload["values"][0] = "reader mutation"
	latest, err := s.GetLatestEvent(ctx, "topic", "stream")
	require.NoError(t, err)
	require.Equal(t, "original", latest.GetEvent().(*memoryTestEvent).Payload["values"][0])
	latest.GetEvent().(*memoryTestEvent).Payload["values"][0] = "latest mutation"
	page, err := s.GetEventsByTopic(ctx, "topic", 0, "")
	require.NoError(t, err)
	require.Equal(t, "original", page.Events[0].GetEvent().(*memoryTestEvent).Payload["values"][0])
	page.Events[0].GetEvent().(*memoryTestEvent).Payload["values"][0] = "page mutation"
	again, err := s.GetEventsByOrgs(ctx, "topic", 0, []string{"envelope-org"}, "")
	require.NoError(t, err)
	require.Equal(t, "original", again.Events[0].GetEvent().(*memoryTestEvent).Payload["values"][0])
}

func TestMemoryAtomicBatchAndTupleIdentity(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "a", "a:b")
	zero, one := 0, 1
	first := memoryWrite("a", "b:c", "org", memoryEventFor("first"))
	first.ExpectedVersion = &zero
	second := memoryWrite("a:b", "c", "org", memoryEventFor("second"))
	second.ExpectedVersion = &zero
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{first, second}))
	for _, write := range []WriteEvents{first, second} {
		read, err := s.GetEvents(ctx, write.Topic, write.StreamName, 0, nil)
		require.NoError(t, err)
		require.Len(t, read, 1)
		require.Equal(t, 1, read[0].GetVersion())
	}
	valid := memoryWrite("a", "new-stream", "org", memoryEventFor("new"))
	conflict := second
	conflict.ExpectedVersion = &zero
	err := s.SaveEvents(ctx, []WriteEvents{valid, conflict})
	var concurrency *OptimisticConcurrencyErr
	require.ErrorAs(t, err, &concurrency)
	require.Equal(t, 0, concurrency.ExpectedVersion)
	require.Equal(t, 1, concurrency.ActualVersion)
	require.Equal(t, "c", concurrency.StreamName)
	absent, err := s.GetLatestEvent(ctx, "a", "new-stream")
	require.NoError(t, err)
	require.Nil(t, absent)
	second.ExpectedVersion = &one
	second.Events = []Event{memoryEventFor("third"), memoryEventFor("fourth")}
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{second}))
	read, err := s.GetEvents(ctx, "a:b", "c", 2, nil)
	require.NoError(t, err)
	require.Len(t, read, 2)
	require.Equal(t, 2, read[0].GetVersion())
	require.Equal(t, 3, read[1].GetVersion())
	limit := 1
	read, err = s.GetEvents(ctx, "a:b", "c", 2, &limit)
	require.NoError(t, err)
	require.Len(t, read, 1)
	require.Equal(t, 2, read[0].GetVersion())
	latest, err := s.GetLatestEvent(ctx, "a", "b:c")
	require.NoError(t, err)
	require.Equal(t, "first", latest.GetEvent().GetAggregateId())
	read, err = s.GetEvents(ctx, "unknown", "stream", 0, nil)
	require.NoError(t, err)
	require.Empty(t, read)
	require.NoError(t, s.SaveEvents(ctx, nil))
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{memoryWrite("a", "empty", "org")}))
}

func TestMemoryWriteValidationDoesNotPartiallyCommit(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("codec failed")
	negative := -1
	var nilEvent *memoryTestEvent
	var nilCodec *memoryTestCodec
	var nilFunctionCodec memoryFunctionCodec
	cases := map[string]func() []WriteEvents{
		"topic":         func() []WriteEvents { return []WriteEvents{memoryWrite("", "s", "org")} },
		"stream":        func() []WriteEvents { return []WriteEvents{memoryWrite("topic", "", "org")} },
		"org":           func() []WriteEvents { return []WriteEvents{memoryWrite("topic", "s", "")} },
		"unknown topic": func() []WriteEvents { return []WriteEvents{memoryWrite("unknown", "s", "org")} },
		"duplicate": func() []WriteEvents {
			return []WriteEvents{memoryWrite("topic", "s", "org"), memoryWrite("topic", "s", "org")}
		},
		"negative version": func() []WriteEvents {
			w := memoryWrite("topic", "s", "org")
			w.ExpectedVersion = &negative
			return []WriteEvents{w}
		},
		"nil event":   func() []WriteEvents { return []WriteEvents{memoryWrite("topic", "s", "org", nil)} },
		"typed nil":   func() []WriteEvents { return []WriteEvents{memoryWrite("topic", "s", "org", nilEvent)} },
		"value event": func() []WriteEvents { return []WriteEvents{memoryWrite("topic", "s", "org", memoryValueEvent{})} },
		"empty type":  func() []WriteEvents { return []WriteEvents{memoryWrite("topic", "s", "org", &memoryTestEvent{})} },
		"tenant mismatch": func() []WriteEvents {
			e := memoryEventFor("id")
			e.Org = "different"
			return []WriteEvents{memoryWrite("topic", "s", "org", e)}
		},
		"nil codec": func() []WriteEvents {
			e := memoryEventFor("id")
			e.NoCodec = true
			return []WriteEvents{memoryWrite("topic", "s", "org", e)}
		},
		"typed nil codec": func() []WriteEvents {
			e := memoryEventFor("id")
			e.Codec = nilCodec
			return []WriteEvents{memoryWrite("topic", "s", "org", e)}
		},
		"typed nil function codec": func() []WriteEvents {
			e := memoryEventFor("id")
			e.Codec = nilFunctionCodec
			return []WriteEvents{memoryWrite("topic", "s", "org", e)}
		},
		"serializer": func() []WriteEvents {
			e := memoryEventFor("id")
			e.Codec = &memoryTestCodec{serialize: func(context.Context, Event) ([]byte, error) { return nil, failure }}
			return []WriteEvents{memoryWrite("topic", "s", "org", e)}
		},
	}
	for name, invalid := range cases {
		t.Run(name, func(t *testing.T) {
			s := memoryStoreFor(t, "topic")
			writes := append([]WriteEvents{memoryWrite("topic", "valid", "org", memoryEventFor("valid"))}, invalid()...)
			err := s.SaveEvents(ctx, writes)
			require.Error(t, err)
			if name == "serializer" {
				require.ErrorIs(t, err, failure)
			}
			page, err := s.GetEventsByTopic(ctx, "topic", 0, "")
			require.NoError(t, err)
			require.Empty(t, page.Events)
			zero := 0
			valid := writes[0]
			valid.ExpectedVersion = &zero
			require.NoError(t, s.SaveEvents(ctx, []WriteEvents{valid}), "failed batch must not advance versions")
		})
	}
	matching := memoryEventFor("matching")
	matching.Org = "org"
	require.NoError(t, memoryStoreFor(t, "topic").SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "s", "org", matching)}))
}

func TestMemoryCodecOwnershipAndReadFailureAtomicity(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "topic")
	event := memoryEventFor("id")
	shared, err := json.Marshal(event)
	require.NoError(t, err)
	codec := &memoryTestCodec{
		serialize: func(context.Context, Event) ([]byte, error) { return shared, nil },
		deserialize: func(_ context.Context, data []byte, e Event) error {
			err := json.Unmarshal(data, e)
			for i := range data {
				data[i] = 'x'
			}
			return err
		},
	}
	event.Codec = codec
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "stream", "org", event)}))
	for i := range shared {
		shared[i] = 'x'
	}
	for i := 0; i < 2; i++ {
		read, err := s.GetEvents(ctx, "topic", "stream", 0, nil)
		require.NoError(t, err)
		require.Equal(t, "id", read[0].GetEvent().GetAggregateId())
	}
	bad := memoryEventFor("bad")
	decodeFailure := errors.New("decode failed")
	bad.Codec = &memoryTestCodec{deserialize: func(context.Context, []byte, Event) error { return decodeFailure }}
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "stream", "org", bad)}))
	read, err := s.GetEvents(ctx, "topic", "stream", 0, nil)
	require.ErrorIs(t, err, decodeFailure)
	require.Nil(t, read)
	latest, err := s.GetLatestEvent(ctx, "topic", "stream")
	require.ErrorIs(t, err, decodeFailure)
	require.Nil(t, latest)
	page, err := s.GetEventsByTopic(ctx, "topic", 0, "")
	require.ErrorIs(t, err, decodeFailure)
	require.Empty(t, page.Events)
	require.Empty(t, page.NextCursor)
}

func TestMemoryPaginationFilteringAndResume(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "topic", "other").WithPageSize(2)
	for i, org := range []string{"a", "b", "a", "b", "a"} {
		require.NoError(t, s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", fmt.Sprint(i), org, memoryEventFor(fmt.Sprint(i)))}))
		// Unrelated topic entries interleave the global append positions.
		require.NoError(t, s.SaveEvents(ctx, []WriteEvents{memoryWrite("other", fmt.Sprint(i), org, memoryEventFor("other"))}))
	}
	page, err := s.GetEventsByOrgs(ctx, "topic", 0, []string{"a", "a"}, "")
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	require.Equal(t, "0", page.Events[0].GetEvent().GetAggregateId())
	require.Equal(t, "2", page.Events[1].GetEvent().GetAggregateId())
	page, err = s.GetEventsByOrgs(ctx, "topic", 0, []string{"a"}, page.NextCursor)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, "4", page.Events[0].GetEvent().GetAggregateId())
	end := page.NextCursor
	page, err = s.GetEventsByOrgs(ctx, "topic", 0, []string{"b"}, end)
	require.NoError(t, err)
	require.Empty(t, page.Events, "a cursor beyond every matching record must not wrap to the beginning")
	require.Equal(t, end, page.NextCursor)
	page, err = s.GetEventsByTopic(ctx, "topic", 0, end)
	require.NoError(t, err)
	require.Empty(t, page.Events)
	require.Equal(t, end, page.NextCursor)
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "new", "b", memoryEventFor("new"))}))
	page, err = s.GetEventsByOrgs(ctx, "topic", 0, []string{"b"}, end)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, "new", page.Events[0].GetEvent().GetAggregateId())
	require.NotEqual(t, end, page.NextCursor)
	page, err = s.GetEventsByOrgs(ctx, "topic", -1, nil, "")
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	empty, err := s.GetEventsByOrgs(ctx, "topic", 0, []string{"absent"}, "")
	require.NoError(t, err)
	require.Empty(t, empty.Events)
	require.Empty(t, empty.NextCursor)
}

func TestMemoryPartitionCountIsPerTopic(t *testing.T) {
	ctx := context.Background()
	s := NewInMemoryEventStore().WithPageSize(100)
	require.NoError(t, s.EnsureTopic(ctx, "one", 1))
	require.NoError(t, s.EnsureTopic(ctx, "seven", 7))
	for i := 0; i < 30; i++ {
		stream := fmt.Sprintf("stream-%d", i)
		one := memoryWrite("one", stream, "org", memoryEventFor(stream))
		seven := memoryWrite("seven", stream, "org", memoryEventFor(stream))
		if i%2 == 0 {
			seven.PartitionKey = "explicit-" + stream
		}
		require.NoError(t, s.SaveEvents(ctx, []WriteEvents{one, seven}))
		key := seven.PartitionKey
		if key == "" {
			key = stream
		}
		partition, err := CalculatePartitionId(key, 7)
		require.NoError(t, err)
		page, err := s.GetEventsByTopic(ctx, "seven", partition, "")
		require.NoError(t, err)
		found := false
		for _, e := range page.Events {
			if e.GetStreamName() == stream {
				found = true
			}
		}
		require.True(t, found, "event was not routed using its topic's partition count")
	}
	one, err := s.GetEventsByTopic(ctx, "one", 0, "")
	require.NoError(t, err)
	require.Len(t, one.Events, 30)
	all, err := s.GetEventsByOrgs(ctx, "seven", -1, nil, "")
	require.NoError(t, err)
	require.Len(t, all.Events, 30)
}

func TestMemoryStreamOwnershipIsImmutableAndBatchAtomic(t *testing.T) {
	ctx := context.Background()
	s := NewInMemoryEventStore()
	require.NoError(t, s.EnsureTopic(ctx, "topic", 3))
	// Find two distinct keys in one bucket, and a key in a different bucket.
	var sameBucket []string
	var differentBucket string
	for i := 0; len(sameBucket) < 2 || differentBucket == ""; i++ {
		key := fmt.Sprintf("routing-key-%d", i)
		partition, err := CalculatePartitionId(key, 3)
		require.NoError(t, err)
		if partition == 0 {
			sameBucket = append(sameBucket, key)
		} else {
			differentBucket = key
		}
	}
	initial := memoryWrite("topic", "stream", "owner", memoryEventFor("first"))
	initial.PartitionKey = sameBucket[0]
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{initial}))
	for _, change := range []struct {
		name, org, partitionKey string
		empty                   bool
	}{
		{"tenant", "other-tenant", sameBucket[0], false},
		{"partition", "owner", differentBucket, false},
		{"tenant on empty append", "other-tenant", sameBucket[0], true},
		{"partition on empty append", "owner", differentBucket, true},
	} {
		t.Run(change.name, func(t *testing.T) {
			appendEvent := memoryWrite("topic", "stream", change.org, memoryEventFor("second"))
			appendEvent.PartitionKey = change.partitionKey
			if change.empty {
				appendEvent.Events = nil
			}
			one := 1
			appendEvent.ExpectedVersion = &one
			peer := memoryWrite("topic", "peer", "owner", memoryEventFor("peer"))
			require.Error(t, s.SaveEvents(ctx, []WriteEvents{peer, appendEvent}))
			missing, err := s.GetLatestEvent(ctx, "topic", "peer")
			require.NoError(t, err)
			require.Nil(t, missing, "an ownership violation must reject the entire batch")
			latest, err := s.GetLatestEvent(ctx, "topic", "stream")
			require.NoError(t, err)
			require.Equal(t, 1, latest.GetVersion())
			require.Equal(t, "owner", latest.GetOrgId())
		})
	}
	// Ownership is the computed partition, not the text of the routing key.
	collision := memoryWrite("topic", "stream", "owner", memoryEventFor("second"))
	collision.PartitionKey = sameBucket[1]
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{collision}))
	page, err := s.GetEventsByTopic(ctx, "topic", 0, "")
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	require.Equal(t, 2, page.Events[1].GetVersion())
	// An empty write neither creates a stream nor claims a tenant or partition.
	empty := memoryWrite("topic", "empty", "initial-owner")
	empty.PartitionKey = sameBucket[0]
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{empty}))
	actual := memoryWrite("topic", "empty", "actual-owner", memoryEventFor("actual"))
	actual.PartitionKey = differentBucket
	zero := 0
	actual.ExpectedVersion = &zero
	require.NoError(t, s.SaveEvents(ctx, []WriteEvents{actual}))
	latest, err := s.GetLatestEvent(ctx, "topic", "empty")
	require.NoError(t, err)
	require.Equal(t, 1, latest.GetVersion())
	require.Equal(t, "actual-owner", latest.GetOrgId())
}

func TestMemoryQueryAndConfigurationValidation(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "topic", "other")
	require.Equal(t, 50, s.pageSize)
	require.Panics(t, func() { s.WithPageSize(0) })
	require.Panics(t, func() { s.WithPageSize(-1) })
	require.NoError(t, s.EnsureTopic(ctx, "topic", 1))
	require.Error(t, s.EnsureTopic(ctx, "topic", 2))
	for _, count := range []int{0, -1, 1 << 32} {
		require.Error(t, s.EnsureTopic(ctx, "bad", count))
	}
	require.Error(t, s.EnsureTopic(ctx, "", 1))
	for _, query := range []struct {
		topic, stream string
		from          int
		limit         *int
	}{
		{"", "s", 0, nil}, {"topic", "", 0, nil}, {"topic", "s", -1, nil},
	} {
		_, err := s.GetEvents(ctx, query.topic, query.stream, query.from, query.limit)
		require.Error(t, err)
	}
	for _, limit := range []int{0, -1} {
		_, err := s.GetEvents(ctx, "topic", "s", 0, &limit)
		require.Error(t, err)
	}
	_, err := s.GetLatestEvent(ctx, "", "s")
	require.Error(t, err)
	_, err = s.GetLatestEvent(ctx, "topic", "")
	require.Error(t, err)
	_, err = s.GetEventsByTopic(ctx, "topic", -1, "")
	require.Error(t, err)
	for _, query := range []struct {
		topic     string
		partition int
	}{{"unknown", 0}, {"", 0}, {"topic", -2}, {"topic", 1}} {
		_, err := s.GetEventsByOrgs(ctx, query.topic, query.partition, nil, "")
		require.Error(t, err)
	}
	invalidCursors := []string{
		"%not-base64", base64.RawURLEncoding.EncodeToString([]byte("not JSON")),
		base64.RawURLEncoding.EncodeToString([]byte(`{"format":2}`)),
		memoryCursorFor("other", 0, 0), memoryCursorFor("topic", 1, 0),
		memoryCursorFor("topic", 0, -1), memoryCursorFor("topic", 0, 1),
	}
	for _, cursor := range invalidCursors {
		_, err := s.GetEventsByTopic(ctx, "topic", 0, cursor)
		require.ErrorIs(t, err, ErrCursorNotFound)
	}
	_, err = s.GetEventsByOrgs(ctx, "topic", -1, nil, memoryCursorFor("topic", 0, 0))
	require.ErrorIs(t, err, ErrCursorNotFound)
}

func TestMemoryAggregateRegistration(t *testing.T) {
	ctx := context.Background()
	s := NewInMemoryEventStore()
	aggregate := &memoryTestAggregate{topic: "registered", partitions: 2, prototype: memoryEventFor("id")}
	require.NoError(t, s.RegisterAggregate(ctx, aggregate))
	require.NoError(t, s.RegisterAggregate(ctx, aggregate))
	prototype, err := s.eventRegistry.GetEventPrototype("memory-event")
	require.NoError(t, err)
	require.IsType(t, &memoryTestEvent{}, prototype)
	aggregate.partitions = 3
	require.Error(t, s.RegisterAggregate(ctx, aggregate))
	var nilAggregate *memoryTestAggregate
	require.Error(t, s.RegisterAggregate(ctx, nilAggregate))
	require.Error(t, s.RegisterAggregate(ctx, nil))
	aggregate.topic = ""
	require.Error(t, s.RegisterAggregate(ctx, aggregate))
	aggregate.topic = "invalid-events"
	aggregate.prototype = nil
	require.Error(t, s.RegisterAggregate(ctx, aggregate))
}

func TestMemorySubscriptionIdentityAndValidation(t *testing.T) {
	ctx := context.Background()
	s := NewInMemoryEventStore()
	cursor, err := s.GetSubscriptionCursor(ctx, "a", "b:c", 0)
	require.ErrorIs(t, err, ErrCursorNotFound)
	require.Empty(t, cursor)
	require.NoError(t, s.SaveSubscriptionCursor(ctx, "a", "b:c", 0, memoryCursorFor("a", 0, 0)))
	require.NoError(t, s.SaveSubscriptionCursor(ctx, "a:b", "c", 0, memoryCursorFor("a:b", 0, 0)))
	require.NoError(t, s.SaveSubscriptionCursor(ctx, "a", "b:c", 1, memoryCursorFor("a", 1, 0)))
	for _, tc := range []struct {
		topic, subscriber string
		partition         int
		expected          string
	}{{"a", "b:c", 0, memoryCursorFor("a", 0, 0)}, {"a:b", "c", 0, memoryCursorFor("a:b", 0, 0)}, {"a", "b:c", 1, memoryCursorFor("a", 1, 0)}} {
		cursor, err := s.GetSubscriptionCursor(ctx, tc.topic, tc.subscriber, tc.partition)
		require.NoError(t, err)
		require.Equal(t, tc.expected, cursor)
	}
	require.NoError(t, s.SaveSubscriptionCursor(ctx, "a", "b:c", 0, ""))
	cursor, err = s.GetSubscriptionCursor(ctx, "a", "b:c", 0)
	require.NoError(t, err)
	require.Empty(t, cursor)
	for _, tc := range []struct {
		topic, subscriber string
		partition         int
	}{{"", "s", 0}, {"t", "", 0}, {"t", "s", -1}} {
		_, err := s.GetSubscriptionCursor(ctx, tc.topic, tc.subscriber, tc.partition)
		require.Error(t, err)
		require.Error(t, s.SaveSubscriptionCursor(ctx, tc.topic, tc.subscriber, tc.partition, "cursor"))
	}
}

func TestMemoryCancellation(t *testing.T) {
	s := memoryStoreFor(t, "topic")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.SaveEvents(ctx, nil), context.Canceled)
	require.ErrorIs(t, s.EnsureTopic(ctx, "new", 1), context.Canceled)
	require.ErrorIs(t, s.SaveSubscriptionCursor(ctx, "topic", "subscriber", 0, "cursor"), context.Canceled)
	_, err := s.GetSubscriptionCursor(ctx, "topic", "subscriber", 0)
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.GetEventsByTopic(ctx, "topic", 0, "")
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.GetEvents(ctx, "topic", "s", 0, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.GetLatestEvent(ctx, "topic", "s")
	require.ErrorIs(t, err, context.Canceled)
	ctx, cancel = context.WithCancel(context.Background())
	e := memoryEventFor("cancel-write")
	e.Codec = &memoryTestCodec{serialize: func(_ context.Context, event Event) ([]byte, error) { cancel(); return json.Marshal(event) }}
	require.ErrorIs(t, s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", "s", "org", e)}), context.Canceled)
	absent, err := s.GetLatestEvent(context.Background(), "topic", "s")
	require.NoError(t, err)
	require.Nil(t, absent)
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprintf("cancel-during-read-%d", count), func(t *testing.T) {
			s := memoryStoreFor(t, "topic")
			readCtx, cancelRead := context.WithCancel(context.Background())
			defer cancelRead()
			event := memoryEventFor("cancel-read")
			event.Codec = &memoryTestCodec{deserialize: func(_ context.Context, data []byte, event Event) error {
				cancelRead()
				return json.Unmarshal(data, event)
			}}
			events := []Event{event}
			if count == 2 {
				events = append(events, memoryEventFor("next"))
			}
			require.NoError(t, s.SaveEvents(context.Background(), []WriteEvents{memoryWrite("topic", "s", "org", events...)}))
			read, err := s.GetEvents(readCtx, "topic", "s", 0, nil)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, read)
		})
	}
}

func TestMemoryConcurrentAccessAndOptimisticAppend(t *testing.T) {
	ctx := context.Background()
	s := memoryStoreFor(t, "topic")
	var successes atomic.Int32
	var group sync.WaitGroup
	for worker := 0; worker < 24; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			zero := 0
			write := memoryWrite("topic", "contended", "org", memoryEventFor(fmt.Sprint(worker)))
			write.ExpectedVersion = &zero
			if err := s.SaveEvents(ctx, []WriteEvents{write}); err == nil {
				successes.Add(1)
			} else {
				var conflict *OptimisticConcurrencyErr
				if !errors.As(err, &conflict) {
					t.Error(err)
				}
			}
			stream := fmt.Sprintf("worker-%d", worker)
			if err := s.SaveEvents(ctx, []WriteEvents{memoryWrite("topic", stream, "org", memoryEventFor(stream))}); err != nil {
				t.Error(err)
			}
			s.WithPageSize(worker + 1)
			if err := s.EnsureTopic(ctx, "topic", 1); err != nil {
				t.Error(err)
			}
			if _, err := s.GetEvents(ctx, "topic", stream, 0, nil); err != nil {
				t.Error(err)
			}
			if _, err := s.GetLatestEvent(ctx, "topic", stream); err != nil {
				t.Error(err)
			}
			page, err := s.GetEventsByTopic(ctx, "topic", 0, "")
			if err != nil {
				t.Error(err)
			}
			if err := s.SaveSubscriptionCursor(ctx, "topic", stream, 0, page.NextCursor); err != nil {
				t.Error(err)
			}
			if _, err := s.GetSubscriptionCursor(ctx, "topic", stream, 0); err != nil {
				t.Error(err)
			}
		}(worker)
	}
	group.Wait()
	require.EqualValues(t, 1, successes.Load())
	s.WithPageSize(100)
	page, err := s.GetEventsByTopic(ctx, "topic", 0, "")
	require.NoError(t, err)
	require.Len(t, page.Events, 25)
}

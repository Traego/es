package es

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAggregateConcurrencyAndAtomicBatch(t *testing.T) {
	ctx := WithCorrelationID(context.Background(), "trace-123")
	events := testMemoryStore(t)
	store := NewAggregateStore(events)
	create := func(id string) *TestAggregate {
		a := NewTestAggregate()
		_, err := a.HandleCommand(ctx, &CreateTestAggCommand{OrgId: "org", AggregateId: id, Name: id})
		require.NoError(t, err)
		require.NoError(t, store.Save(ctx, a))
		return a
	}
	create("a")
	create("b")
	a, b, stale := NewTestAggregate(), NewTestAggregate(), NewTestAggregate()
	require.NoError(t, store.Load(ctx, "org", "a", a))
	require.NoError(t, store.Load(ctx, "org", "b", b))
	require.NoError(t, store.Load(ctx, "org", "a", stale))
	for _, agg := range []*TestAggregate{a, b, stale} {
		_, err := agg.HandleCommand(ctx, &ActivateCommand{OrgId: "org", AggregateId: agg.Id})
		require.NoError(t, err)
	}
	require.NoError(t, store.Save(ctx, a))
	err := store.SaveBatch(ctx, []AggregateRoot{b, stale})
	require.ErrorIs(t, err, ErrOptimisticConcurrency)
	var conflict *OptimisticConcurrencyErr
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, 1, conflict.ExpectedVersion)
	require.Equal(t, 2, conflict.ActualVersion)
	require.Contains(t, conflict.Error(), "expected_version=1")
	require.Len(t, b.GetUncommittedEvents(), 1)
	require.Len(t, stale.GetUncommittedEvents(), 1)
	fresh := NewTestAggregate()
	require.NoError(t, store.Load(ctx, "org", "b", fresh))
	require.Equal(t, 1, fresh.Version)
	require.NoError(t, store.Save(ctx, b))
	stream, err := GetStreamNameFromFullAgg(b)
	require.NoError(t, err)
	history, err := events.GetEvents(ctx, b.GetTopic(), stream, 0, nil)
	require.NoError(t, err)
	for _, e := range history {
		require.Equal(t, "trace-123", e.GetCorrelationId())
		require.Equal(t, stream, e.GetStreamName())
		require.Equal(t, b.GetTopic(), e.GetTopic())
		require.False(t, e.GetGlobalPosition().IsZero())
	}
	err = store.Save(ctx, stale)
	require.ErrorIs(t, err, ErrOptimisticConcurrency)
	require.Len(t, stale.GetUncommittedEvents(), 1)
	// Creation also checks expected version zero.
	duplicate := NewTestAggregate()
	_, err = duplicate.HandleCommand(ctx, &CreateTestAggCommand{OrgId: "org", AggregateId: "a"})
	require.NoError(t, err)
	require.ErrorIs(t, store.Save(ctx, duplicate), ErrOptimisticConcurrency)
}

func TestAggregateValidationAndReplay(t *testing.T) {
	ctx := context.Background()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	store := NewAggregateStore(testMemoryStore(t))
	root := NewTestAggregate()
	require.ErrorIs(t, store.Load(canceled, "org", "id", root), context.Canceled)
	require.ErrorIs(t, store.SaveBatch(canceled, nil), context.Canceled)
	require.Error(t, NewAggregateStore(nil).Load(ctx, "org", "id", root))
	require.Error(t, NewAggregateStore(nil).SaveBatch(ctx, nil))
	require.Error(t, store.Load(ctx, "org", "id", nil))
	require.Error(t, store.Save(ctx, nil))
	require.Error(t, store.SaveBatch(ctx, []AggregateRoot{nil}))
	root.Id = "id"
	root.OrgId = "org"
	root.Version = 1
	require.Error(t, store.Load(ctx, "org", "id", root))
	root.Version = 0
	root.uncommittedEvents = []Event{&TestAggregateCreated{}}
	require.Error(t, store.Load(ctx, "org", "id", root))
	require.Error(t, store.Save(ctx, root))
	require.Len(t, root.GetUncommittedEvents(), 1)
	// A caller cannot change the pending slice through the getter.
	pending := root.GetUncommittedEvents()
	pending[0] = nil
	require.NotNil(t, root.GetUncommittedEvents()[0])
	base := &AggregateBase{}
	require.Same(t, base, base.getBase())
	base.RecordEvent(&TestAggregateCreated{})
	require.Empty(t, base.GetUncommittedEvents())
	require.ErrorIs(t, base.LoadHistoricEvents(canceled, nil, root), context.Canceled)
	require.Error(t, base.LoadHistoricEvents(ctx, nil, nil))
	require.Error(t, base.LoadHistoricEvents(ctx, []Envelope{NewEnvelope("t", "o", "s", time.Now(), 1, nil, "")}, root))
	require.ErrorIs(t, ApplyEvent(canceled, root, &TestAggregateCreated{}), context.Canceled)
	require.Error(t, ApplyEvent(ctx, nil, &TestAggregateCreated{}))
	require.Error(t, ApplyEvent(ctx, root, nil))
	failing := NewMockAggregate("id", true)
	require.Error(t, ApplyEvent(ctx, failing, &TestAggregateCreated{}))
	require.Empty(t, failing.GetUncommittedEvents())
	good := NewMockAggregate("id", false)
	env := func(v int) Envelope {
		return NewEnvelope("test-topic", "org", "id", time.Now(), v, &MockEvent{EventType: "test1"}, "")
	}
	require.Error(t, good.LoadHistoricEvents(ctx, []Envelope{env(1), env(3)}, good))
	require.Zero(t, good.Version)
	require.Empty(t, good.processedEventTypes)
	original := []Envelope{env(2), env(1)}
	require.NoError(t, good.LoadHistoricEvents(ctx, original, good))
	require.Equal(t, 2, original[0].GetVersion())
	require.NoError(t, good.LoadHistoricEvents(ctx, []Envelope{env(3)}, good))
	require.Equal(t, 3, good.Version)
}

func TestStreamNamesAndPartitionValidation(t *testing.T) {
	require.Empty(t, CorrelationID(context.Background()))
	a := NewMockAggregate("id", false)
	_, err := GetStreamNameFromFullAgg(nil)
	require.Error(t, err)
	var nilAggregate *TestAggregate
	_, err = GetStreamName(nilAggregate, "org", "id")
	require.Error(t, err)
	a.aggregateType = ""
	_, err = GetStreamName(a, "org", "id")
	require.Error(t, err)
	a.aggregateType = "a/b"
	one, err := GetStreamName(a, "c", "d")
	require.NoError(t, err)
	a.aggregateType = "a"
	two, err := GetStreamName(a, "b/c", "d")
	require.NoError(t, err)
	require.NotEqual(t, one, two)
	require.Equal(t, 2, strings.Count(one, "/"))
	for _, count := range []int{-1, 0, int(1 << 32)} {
		_, err := CalculatePartitionId("key", count)
		require.Error(t, err)
	}
	_, err = CalculatePartitionId("", 1)
	require.Error(t, err)
	x, err := CalculatePartitionId("key", 16)
	require.NoError(t, err)
	require.GreaterOrEqual(t, x, 0)
	require.Less(t, x, 16)
	y, err := CalculatePartitionId("key", 16)
	require.NoError(t, err)
	require.Equal(t, x, y)
}

type registryAggregate struct {
	TestAggregate
	prototypes            []Event
	emptyType, emptyTopic bool
}

func (a *registryAggregate) RegisterEvents(register func(Event)) {
	for _, e := range a.prototypes {
		register(e)
	}
}
func (a *registryAggregate) GetAggregateType() string {
	if a.emptyType {
		return ""
	}
	return "registry-aggregate"
}
func (a *registryAggregate) GetTopic() string {
	if a.emptyTopic {
		return ""
	}
	return "registry-topic"
}

type registryEvent struct {
	Name        string
	Codec       EventCodec `json:"-"`
	Unsupported chan int   `json:"unsupported,omitempty"`
}

func (e *registryEvent) GetEventType() string      { return e.Name }
func (e *registryEvent) GetAggregateId() string    { return "id" }
func (e *registryEvent) GetEventCodec() EventCodec { return e.Codec }

type registryOther struct{ registryEvent }
type registryValue struct{}

func (registryValue) GetEventType() string      { return "value" }
func (registryValue) GetAggregateId() string    { return "id" }
func (registryValue) GetEventCodec() EventCodec { return JsonEventCodec{} }

func TestRegistryValidationAtomicityAndIsolation(t *testing.T) {
	ctx := context.Background()
	r := NewEventRegistry()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	valid := func() *registryAggregate { return &registryAggregate{prototypes: []Event{&TestAggregateCreated{}}} }
	require.ErrorIs(t, r.RegisterAggregate(canceled, valid()), context.Canceled)
	require.Error(t, r.RegisterAggregate(ctx, nil))
	a := valid()
	a.emptyType = true
	require.Error(t, r.RegisterAggregate(ctx, a))
	a.emptyType = false
	a.emptyTopic = true
	require.Error(t, r.RegisterAggregate(ctx, a))
	var nilEvent *TestAggregateCreated
	var nilCodec *JsonEventCodec
	for _, prototype := range []Event{nil, nilEvent, registryValue{}, &registryEvent{Name: "", Codec: JsonEventCodec{}}, &registryEvent{Name: "nil-codec"}, &registryEvent{Name: "typed-nil-codec", Codec: nilCodec}} {
		a = valid()
		a.prototypes = append(a.prototypes, prototype)
		require.Error(t, r.RegisterAggregate(ctx, a))
		_, err := r.GetEventPrototype("test-aggregate-created")
		require.Error(t, err)
	}
	a = valid()
	a.prototypes = []Event{&registryEvent{Name: "collision", Codec: JsonEventCodec{}}, &registryOther{registryEvent{Name: "collision", Codec: JsonEventCodec{}}}}
	require.Error(t, r.RegisterAggregate(ctx, a))
	a.prototypes = a.prototypes[:1]
	require.NoError(t, r.RegisterAggregate(ctx, a))
	a.prototypes = []Event{&registryOther{registryEvent{Name: "collision", Codec: JsonEventCodec{}}}}
	require.Error(t, r.RegisterAggregate(ctx, a))
	require.NoError(t, r.RegisterAggregate(ctx, valid()))
	require.NoError(t, r.RegisterAggregate(ctx, valid()))
	types := r.GetAggregatesForTopic("registry-topic")
	require.Len(t, types, 1)
	types[0] = "corrupted"
	require.Equal(t, []string{"registry-aggregate"}, r.GetAggregatesForTopic("registry-topic"))
	require.Empty(t, r.GetAggregatesForTopic("unknown"))
	e, err := r.DeserializeEvent(ctx, "test-aggregate-created", []byte(`{"name":"persisted","aggregateId":"a"}`))
	require.NoError(t, err)
	require.Equal(t, "persisted", e.(*TestAggregateCreated).Name)
	_, err = r.DeserializeEvent(ctx, "test-aggregate-created", []byte("{"))
	require.Error(t, err)
	_, err = r.DeserializeEvent(ctx, "unknown", nil)
	require.Error(t, err)
	_, err = r.DeserializeEvent(canceled, "test-aggregate-created", nil)
	require.ErrorIs(t, err, context.Canceled)
	prototype, err := r.GetEventPrototype("test-aggregate-created")
	require.NoError(t, err)
	prototype.(*TestAggregateCreated).Name = "changed"
	fresh, err := r.GetEventPrototype("test-aggregate-created")
	require.NoError(t, err)
	require.Empty(t, fresh.(*TestAggregateCreated).Name)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if err := r.RegisterAggregate(ctx, valid()); err != nil {
					t.Error(err)
				}
				r.GetAggregatesForTopic("registry-topic")
			}
		}()
	}
	wg.Wait()
	codec := JsonEventCodec{}
	_, err = codec.Serialize(ctx, &registryEvent{Unsupported: make(chan int)})
	require.Error(t, err)
	require.Error(t, codec.Deserialize(ctx, []byte("{"), &TestAggregateCreated{}))
}

type brokenAggregateReader struct {
	AggregateStore
	err error
}

func (s brokenAggregateReader) Load(context.Context, string, string, AggregateRoot) error {
	return s.err
}

func TestMaterializerConfigurationAndLoadError(t *testing.T) {
	ctx := context.Background()
	m := NewMaterializerWithContext(ctx, "sub", "test-topic", []int{0}, func(context.Context, AggregateRoot, int) error { return nil }, NewTestAggregate(), NewAggregateStore(testMemoryStore(t)), true, nil, WithMaterializerPollingTime(time.Millisecond)).(*materializer)
	require.Equal(t, time.Millisecond, m.pollingTime)
	require.Error(t, m.processEvent(ctx, Envelope{}))
	failure := errors.New("load failed")
	m.aggregateStore = brokenAggregateReader{err: failure}
	err := m.processEvent(ctx, NewEnvelope("test-topic", "org", "TestAggregate/org/id", time.Now(), 1, &TestAggregateCreated{AggregateId: "id"}, ""))
	require.ErrorIs(t, err, failure)
}

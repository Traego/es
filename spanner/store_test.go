package spannerstore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/traego/es"
)

var errInjected = errors.New("injected transport failure")

type readAnswer struct {
	rows []*spanner.Row
	err  error
}
type fakeBackend struct {
	answers                                   []readAnswer
	statements                                []spanner.Statement
	mutations                                 []*spanner.Mutation
	transactionError, bufferError, applyError error
	transactions                              int
}

func (f *fakeBackend) read(_ context.Context, stmt spanner.Statement) ([]*spanner.Row, error) {
	f.statements = append(f.statements, stmt)
	if len(f.answers) == 0 {
		panic("unexpected read: " + stmt.SQL)
	}
	a := f.answers[0]
	f.answers = f.answers[1:]
	return a.rows, a.err
}
func (f *fakeBackend) transact(_ context.Context, fn func(transaction) error) error {
	f.transactions++
	if f.transactionError != nil {
		return f.transactionError
	}
	return fn(f)
}
func (f *fakeBackend) buffer(m []*spanner.Mutation) error {
	f.mutations = append(f.mutations, m...)
	return f.bufferError
}
func (f *fakeBackend) apply(_ context.Context, m []*spanner.Mutation) error {
	f.mutations = append(f.mutations, m...)
	return f.applyError
}

type testEvent struct {
	ID          string        `json:"id"`
	Org         string        `json:"org"`
	Name        string        `json:"name"`
	EventType   string        `json:"-"`
	InvalidType bool          `json:"-"`
	NilCodec    bool          `json:"-"`
	Codec       es.EventCodec `json:"-"`
}

func (e testEvent) GetAggregateId() string { return e.ID }
func (e testEvent) GetOrgId() string       { return e.Org }
func (e testEvent) GetEventType() string {
	if e.InvalidType {
		return ""
	}
	if e.EventType != "" {
		return e.EventType
	}
	return "test.created"
}
func (e testEvent) GetEventCodec() es.EventCodec {
	if e.NilCodec {
		return nil
	}
	if e.Codec != nil {
		return e.Codec
	}
	return es.JsonEventCodec{}
}

type failingCodec struct{}

func (*failingCodec) Serialize(context.Context, es.Event) ([]byte, error) { return nil, errInjected }
func (*failingCodec) Deserialize(context.Context, []byte, es.Event) error { return errInjected }

type testAggregate struct {
	es.AggregateBase
	topic      string
	count      int
	prototypes []es.Event
	invalid    bool
}

func (a *testAggregate) GetAggregateType() string {
	if a.invalid {
		return ""
	}
	return "test"
}
func (a *testAggregate) GetTopic() string       { return a.topic }
func (a *testAggregate) GetPartitionCount() int { return a.count }
func (a *testAggregate) InitialState() es.AggregateRoot {
	return &testAggregate{topic: a.topic, count: a.count}
}
func (a *testAggregate) HandleCommand(context.Context, interface{}) ([]es.Event, error) {
	return nil, nil
}
func (a *testAggregate) HandleEvent(context.Context, es.Event, *es.Envelope) error { return nil }
func (a *testAggregate) RegisterEvents(register func(es.Event)) {
	for _, e := range a.prototypes {
		register(e)
	}
}

func row(t *testing.T, names []string, values ...any) *spanner.Row {
	t.Helper()
	r, err := spanner.NewRow(names, values)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func answer(t *testing.T, value any) readAnswer {
	return readAnswer{rows: []*spanner.Row{row(t, []string{"value"}, value)}}
}

func streamAnswer(t *testing.T, version int64, org string, partition int64) readAnswer {
	return readAnswer{rows: []*spanner.Row{row(t, []string{"version", "org_id", "partition_id"}, version, org, partition)}}
}
func eventRow(t *testing.T, stamp time.Time, eventType string, payload []byte) *spanner.Row {
	return row(t, strings.Split(eventColumns, ", "), "topic", "org", "stream", stamp, int64(1), eventType, payload, "correlation")
}
func unitStore(t *testing.T, answers ...readAnswer) (*Store, *fakeBackend) {
	t.Helper()
	s, err := New(&spanner.Client{}, Config{PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeBackend{answers: answers}
	s.db = f
	if err := s.registry.RegisterAggregate(context.Background(), &testAggregate{topic: "topic", count: 1, prototypes: []es.Event{&testEvent{}}}); err != nil {
		t.Fatal(err)
	}
	return s, f
}
func goodWrite() es.WriteEvents {
	return es.WriteEvents{Topic: "topic", StreamName: "stream", OrgId: "org", Events: []es.Event{&testEvent{ID: "stream", Name: "first"}}}
}
func requireError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
}
func requireOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func intPtr(value int) *int { return &value }

func TestNewAndSchemaValidation(t *testing.T) {
	_, err := New(nil, Config{})
	requireError(t, err)
	_, err = New(&spanner.Client{}, Config{PageSize: -1})
	requireError(t, err)
	s, err := New(&spanner.Client{}, Config{})
	requireOK(t, err)
	if s.pageSize != 50 {
		t.Fatal(s.pageSize)
	}
	if len(SchemaDDL()) != 5 {
		t.Fatal(SchemaDDL())
	}
	if !strings.Contains(SchemaDDL()[0], "PRIMARY KEY (topic, stream_name, version)") {
		t.Fatal("missing topic namespace")
	}
	requireError(t, CreateSchema(context.Background(), nil, "db"))
	if !isNil((*testEvent)(nil)) || !isNil(nil) || isNil(testEvent{}) {
		t.Fatal("incorrect nil check")
	}
}

func TestEnsureTopicValidationAndFailures(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		topic string
		count int
	}{{"", 1}, {"  ", 1}, {"topic", 0}, {"topic", -1}, {"topic", 1 << 32}} {
		s, _ := unitStore(t)
		requireError(t, s.EnsureTopic(ctx, tc.topic, tc.count))
	}
	for _, tc := range []struct {
		name      string
		input     readAnswer
		count     int
		bufferErr error
		wantErr   bool
	}{
		{"create", readAnswer{}, 3, nil, false},
		{"buffer failure", readAnswer{}, 3, errInjected, true},
		{"read failure", readAnswer{err: errInjected}, 3, nil, true},
		{"bad row", answer(t, "bad"), 3, nil, true},
		{"same", answer(t, int64(3)), 3, nil, false},
		{"repartition", answer(t, int64(3)), 4, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f := unitStore(t, tc.input)
			f.bufferError = tc.bufferErr
			err := s.EnsureTopic(ctx, "topic", tc.count)
			if tc.wantErr {
				requireError(t, err)
			} else {
				requireOK(t, err)
			}
		})
	}
	s, f := unitStore(t)
	f.transactionError = errInjected
	if !errors.Is(s.EnsureTopic(ctx, "topic", 3), errInjected) {
		t.Fatal("transaction error lost")
	}
}

func TestRegisterAggregate(t *testing.T) {
	ctx := context.Background()
	s, _ := unitStore(t, readAnswer{})
	requireError(t, s.RegisterAggregate(ctx, nil))
	requireError(t, s.RegisterAggregate(ctx, (*testAggregate)(nil)))
	requireError(t, s.RegisterAggregate(ctx, &testAggregate{invalid: true, topic: "topic"}))
	requireOK(t, s.RegisterAggregate(ctx, &testAggregate{topic: "topic", count: 1, prototypes: []es.Event{&testEvent{}}}))
}

func TestSaveValidation(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		mutate func(*es.WriteEvents)
	}{
		{"topic", func(w *es.WriteEvents) { w.Topic = "" }},
		{"stream", func(w *es.WriteEvents) { w.StreamName = "" }},
		{"org", func(w *es.WriteEvents) { w.OrgId = "" }},
		{"version", func(w *es.WriteEvents) { w.ExpectedVersion = intPtr(-1) }},
		{"nil event", func(w *es.WriteEvents) { w.Events = []es.Event{nil} }},
		{"typed nil", func(w *es.WriteEvents) { w.Events = []es.Event{(*testEvent)(nil)} }},
		{"type", func(w *es.WriteEvents) { w.Events = []es.Event{&testEvent{InvalidType: true}} }},
		{"codec", func(w *es.WriteEvents) { w.Events = []es.Event{&testEvent{NilCodec: true}} }},
		{"typed codec", func(w *es.WriteEvents) { w.Events = []es.Event{&testEvent{Codec: (*failingCodec)(nil)}} }},
		{"tenant", func(w *es.WriteEvents) { w.Events = []es.Event{&testEvent{Org: "other"}} }},
		{"serialization", func(w *es.WriteEvents) { w.Events = []es.Event{&testEvent{Codec: &failingCodec{}}} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, f := unitStore(t)
			w := goodWrite()
			tc.mutate(&w)
			requireError(t, s.SaveEvents(ctx, []es.WriteEvents{w}))
			if f.transactions != 0 {
				t.Fatal("opened transaction for invalid batch")
			}
		})
	}
	s, f := unitStore(t)
	w := goodWrite()
	requireError(t, s.SaveEvents(ctx, []es.WriteEvents{w, w}))
	requireOK(t, s.SaveEvents(ctx, nil))
	if f.transactions != 0 {
		t.Fatal("unexpected transaction")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if !errors.Is(s.SaveEvents(canceled, nil), context.Canceled) {
		t.Fatal("canceled empty write")
	}
}

func TestSaveTransactionalFailures(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		answers   []readAnswer
		version   *int
		bufferErr error
	}{
		{"topic read", []readAnswer{{err: errInjected}}, nil, nil},
		{"unknown topic", []readAnswer{{}}, nil, nil},
		{"topic row", []readAnswer{answer(t, "bad")}, nil, nil},
		{"partition count", []readAnswer{answer(t, int64(0))}, nil, nil},
		{"version read", []readAnswer{answer(t, int64(1)), {err: errInjected}}, nil, nil},
		{"extra version result", []readAnswer{answer(t, int64(1)), {rows: []*spanner.Row{row(t, []string{"v"}, int64(1)), row(t, []string{"v"}, int64(2))}}}, nil, nil},
		{"version row", []readAnswer{answer(t, int64(1)), answer(t, "bad")}, nil, nil},
		{"conflict", []readAnswer{answer(t, int64(1)), streamAnswer(t, 2, "org", 0)}, intPtr(1), nil},
		{"buffer", []readAnswer{answer(t, int64(1)), streamAnswer(t, 2, "org", 0)}, intPtr(2), errInjected},
		{"tenant change", []readAnswer{answer(t, int64(1)), streamAnswer(t, 2, "other", 0)}, intPtr(2), nil},
		{"partition change", []readAnswer{answer(t, int64(2)), streamAnswer(t, 2, "org", 100)}, intPtr(2), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, f := unitStore(t, tc.answers...)
			f.bufferError = tc.bufferErr
			w := goodWrite()
			w.ExpectedVersion = tc.version
			err := s.SaveEvents(ctx, []es.WriteEvents{w})
			requireError(t, err)
			if tc.name == "conflict" {
				var conflict *es.OptimisticConcurrencyErr
				if !errors.As(err, &conflict) || conflict.ActualVersion != 2 {
					t.Fatalf("wrong conflict: %v", err)
				}
			}
		})
	}
}

func TestSaveBatchUsesTopicNamespaceAndAtomicBuffer(t *testing.T) {
	s, f := unitStore(t, answer(t, int64(1)), readAnswer{}, answer(t, int64(1)), streamAnswer(t, 4, "org", 0))
	first := goodWrite()
	first.ExpectedVersion = intPtr(0)
	second := goodWrite()
	second.Topic = "other"
	second.PartitionKey = "explicit"
	second.ExpectedVersion = intPtr(4)
	second.Events = append(second.Events, &testEvent{Org: "org", Name: "second"})
	requireOK(t, s.SaveEvents(context.Background(), []es.WriteEvents{first, second}))
	if len(f.mutations) != 3 || f.transactions != 1 {
		t.Fatalf("mutations=%d transactions=%d", len(f.mutations), f.transactions)
	}
	for _, index := range []int{1, 3} {
		if !strings.Contains(f.statements[index].SQL, "topic = @topic AND stream_name = @stream") {
			t.Fatal(f.statements[index].SQL)
		}
	}
	if f.statements[1].Params["topic"] != "topic" || f.statements[3].Params["topic"] != "other" {
		t.Fatal("namespace parameters lost")
	}
	s, _ = unitStore(t, answer(t, int64(1)), readAnswer{})
	first.Events = nil
	requireOK(t, s.SaveEvents(context.Background(), []es.WriteEvents{first}))
}

func TestReadEvents(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		topic, stream string
		from          int
		limit         *int
	}{{"", "stream", 0, nil}, {"topic", "", 0, nil}, {"topic", "stream", -1, nil}, {"topic", "stream", 0, intPtr(0)}} {
		s, _ := unitStore(t)
		_, err := s.GetEvents(ctx, tc.topic, tc.stream, tc.from, tc.limit)
		requireError(t, err)
	}
	for _, a := range []readAnswer{{err: errInjected}, answer(t, "invalid row"), {rows: []*spanner.Row{eventRow(t, time.Now(), "unregistered", []byte(`{}`))}}} {
		s, _ := unitStore(t, a)
		_, err := s.GetEvents(ctx, "topic", "stream", 0, nil)
		requireError(t, err)
	}
	s, f := unitStore(t, readAnswer{rows: []*spanner.Row{eventRow(t, time.Now(), "test.created", []byte(`{"name":"first"}`))}})
	events, err := s.GetEvents(ctx, "topic", "stream", 1, intPtr(2))
	requireOK(t, err)
	if len(events) != 1 || events[0].GetEvent().(*testEvent).Name != "first" || events[0].GetCorrelationId() != "correlation" {
		t.Fatalf("bad decoded events: %v", events)
	}
	if f.statements[0].Params["limit"] != int64(2) || f.statements[0].Params["version"] != int64(1) {
		t.Fatal(f.statements[0])
	}
}

func TestLatestEvent(t *testing.T) {
	ctx := context.Background()
	s, _ := unitStore(t)
	_, err := s.GetLatestEvent(ctx, "", "stream")
	requireError(t, err)
	s, _ = unitStore(t, readAnswer{err: errInjected})
	_, err = s.GetLatestEvent(ctx, "topic", "stream")
	requireError(t, err)
	s, _ = unitStore(t, readAnswer{})
	event, err := s.GetLatestEvent(ctx, "topic", "stream")
	requireOK(t, err)
	if event != nil {
		t.Fatal("expected missing event")
	}
	s, _ = unitStore(t, readAnswer{rows: []*spanner.Row{eventRow(t, time.Now(), "test.created", []byte(`{}`))}})
	event, err = s.GetLatestEvent(ctx, "topic", "stream")
	requireOK(t, err)
	if event == nil || event.GetVersion() != 1 {
		t.Fatal("missing latest")
	}
}

func TestCursorValidation(t *testing.T) {
	stamp := time.Date(2025, 1, 2, 3, 4, 5, 6, time.UTC)
	data := cursorData{Topic: "topic", Partition: 0, Timestamp: stamp, Stream: "stream", Version: 1}
	encoded := encodeCursor(data)
	decoded, err := decodeCursor(encoded, "topic", 0)
	requireOK(t, err)
	if decoded != data {
		t.Fatalf("roundtrip: %#v", decoded)
	}
	for _, value := range []string{"!", base64.RawURLEncoding.EncodeToString([]byte("not-json")), base64.RawURLEncoding.EncodeToString([]byte(`{}`))} {
		_, err := decodeCursor(value, "topic", 0)
		requireError(t, err)
	}
	_, err = decodeCursor(encoded, "other", 0)
	requireError(t, err)
	_, err = decodeCursor(encoded, "topic", 1)
	requireError(t, err)
	data.Timestamp = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = decodeCursor(encodeCursor(data), "topic", 0)
	requireError(t, err)
}

func TestPagingAndCursorPersistence(t *testing.T) {
	ctx := context.Background()
	stamp := time.Date(2025, 1, 2, 3, 4, 5, 6, time.UTC)
	s, f := unitStore(t, answer(t, int64(1)), readAnswer{rows: []*spanner.Row{eventRow(t, stamp, "test.created", []byte(`{}`))}}, answer(t, int64(1)), readAnswer{})
	page, err := s.GetEventsByTopic(ctx, "topic", 0, "")
	requireOK(t, err)
	if len(page.Events) != 1 || page.NextCursor == "" {
		t.Fatal("missing first page")
	}
	end, err := s.GetEventsByOrgs(ctx, "topic", 0, []string{"org"}, page.NextCursor)
	requireOK(t, err)
	if len(end.Events) != 0 || end.NextCursor != page.NextCursor {
		t.Fatal("cursor lost at end")
	}
	if _, ok := f.statements[3].Params["timestamp"].(time.Time); !ok {
		t.Fatal("timestamp must be typed")
	}
	if !strings.Contains(f.statements[3].SQL, "org_id IN UNNEST(@orgs)") {
		t.Fatal("missing tenant filter")
	}
	s, f = unitStore(t, answer(t, int64(1)), readAnswer{})
	_, err = s.GetEventsByOrgs(ctx, "topic", -1, nil, "")
	requireOK(t, err)
	if strings.Contains(f.statements[1].SQL, "partition_id =") {
		t.Fatal("all partitions filtered")
	}
	s, _ = unitStore(t)
	_, err = s.GetEventsByTopic(ctx, "topic", -1, "")
	requireError(t, err)
	_, err = s.GetEventsByOrgs(ctx, "", 0, nil, "")
	requireError(t, err)
	_, err = s.GetEventsByOrgs(ctx, "topic", -2, nil, "")
	requireError(t, err)
	_, err = s.GetEventsByOrgs(ctx, "topic", 0, nil, "!")
	requireError(t, err)
	s, _ = unitStore(t, answer(t, int64(1)), readAnswer{err: errInjected})
	_, err = s.GetEventsByTopic(ctx, "topic", 0, "")
	requireError(t, err)
	s, _ = unitStore(t, answer(t, int64(1)), readAnswer{rows: []*spanner.Row{eventRow(t, time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), "test.created", []byte(`{}`))}})
	_, err = s.GetEventsByTopic(ctx, "topic", 0, "")
	requireError(t, err)
}

func TestPagingRejectsUnknownOrInvalidPartitions(t *testing.T) {
	for _, tc := range []struct {
		answer    readAnswer
		partition int
	}{
		{readAnswer{err: errInjected}, 0}, {readAnswer{}, 0}, {answer(t, "bad"), 0},
		{answer(t, int64(0)), 0}, {answer(t, int64(1)), 1}, {answer(t, int64(1<<32)), 0},
	} {
		s, _ := unitStore(t, tc.answer)
		_, err := s.GetEventsByOrgs(context.Background(), "topic", tc.partition, nil, "")
		requireError(t, err)
	}
}

func TestSubscriptionCursors(t *testing.T) {
	ctx := context.Background()
	s, _ := unitStore(t)
	_, err := s.GetSubscriptionCursor(ctx, "", "subscriber", 0)
	requireError(t, err)
	requireError(t, s.SaveSubscriptionCursor(ctx, "topic", "", 0, ""))
	requireError(t, s.SaveSubscriptionCursor(ctx, "topic", "subscriber", -1, ""))
	requireError(t, s.SaveSubscriptionCursor(ctx, "topic", "subscriber", 0, "!"))
	s, _ = unitStore(t, readAnswer{err: errInjected})
	_, err = s.GetSubscriptionCursor(ctx, "topic", "subscriber", 0)
	requireError(t, err)
	s, _ = unitStore(t, readAnswer{})
	_, err = s.GetSubscriptionCursor(ctx, "topic", "subscriber", 0)
	if !errors.Is(err, es.ErrCursorNotFound) {
		t.Fatalf("missing: %v", err)
	}
	s, _ = unitStore(t, answer(t, int64(1)))
	_, err = s.GetSubscriptionCursor(ctx, "topic", "subscriber", 0)
	requireError(t, err)
	s, f := unitStore(t, answer(t, "cursor"))
	cursor, err := s.GetSubscriptionCursor(ctx, "topic", "subscriber", 0)
	requireOK(t, err)
	if cursor != "cursor" {
		t.Fatal(cursor)
	}
	requireOK(t, s.SaveSubscriptionCursor(ctx, "topic", "subscriber", 0, ""))
	valid := encodeCursor(cursorData{Topic: "topic", Partition: 0, Timestamp: time.Now(), Stream: "stream", Version: 1})
	requireOK(t, s.SaveSubscriptionCursor(ctx, "topic", "subscriber", 0, valid))
	if len(f.mutations) != 2 {
		t.Fatal("cursor not persisted")
	}
	f.applyError = errInjected
	if !errors.Is(s.SaveSubscriptionCursor(ctx, "topic", "subscriber", 0, valid), errInjected) {
		t.Fatal("write error lost")
	}
}

func ExampleSchemaDDL() {
	fmt.Println(len(SchemaDDL()))
	// Output: 5
}

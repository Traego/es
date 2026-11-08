package sqlitestore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/traego/es"
	modernsqlite "modernc.org/sqlite"
)

var injected = errors.New("injected database failure")

type faultPlan struct {
	fail  func(string, string) error
	alter func(string, []driver.Value)
}
type faultConnector struct {
	driver.Connector
	plan *faultPlan
}

func (c faultConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return faultConn{conn, c.plan}, nil
}

type faultConn struct {
	driver.Conn
	plan *faultPlan
}

func (c faultConn) Unwrap() driver.Conn { return c.Conn }
func (c faultConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if c.plan.fail != nil {
		if err := c.plan.fail("exec", q); err != nil {
			return nil, err
		}
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}
func (c faultConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if c.plan.fail != nil {
		if err := c.plan.fail("query", q); err != nil {
			return nil, err
		}
	}
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	if err != nil {
		return nil, err
	}
	return &faultRows{Rows: rows, query: q, plan: c.plan}, nil
}
func (c faultConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
}
func (c faultConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, q)
}
func (c faultConn) Ping(ctx context.Context) error { return c.Conn.(driver.Pinger).Ping(ctx) }
func (c faultConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}
func (c faultConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }

type faultRows struct {
	driver.Rows
	query string
	plan  *faultPlan
}

func (r *faultRows) Next(values []driver.Value) error {
	if r.plan.fail != nil {
		if err := r.plan.fail("next", r.query); err != nil {
			return err
		}
	}
	err := r.Rows.Next(values)
	if err == nil && r.plan.alter != nil {
		r.plan.alter(r.query, values)
	}
	return err
}
func faultOpen(t *testing.T, plan *faultPlan) (*Store, error) {
	t.Helper()
	return openStore(context.Background(), filepath.Join(t.TempDir(), "fault.db"), Config{MaxOpenConns: 1}, func(_, dsn string) (*sql.DB, error) {
		connector, err := modernsqlite.NewConnector(dsn)
		if err != nil {
			return nil, err
		}
		return sql.OpenDB(faultConnector{connector, plan}), nil
	})
}
func faultStore(t *testing.T) (*Store, *faultPlan) {
	t.Helper()
	p := &faultPlan{}
	s, err := faultOpen(t, p)
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	must(t, s.RegisterAggregate(context.Background(), &aggregate{topic: "topic", count: 1}))
	return s, p
}
func failSQL(op, prefix string) func(string, string) error {
	return func(got, q string) error {
		if got == op && strings.HasPrefix(q, prefix) {
			return injected
		}
		return nil
	}
}

func TestOpenAndMigrationFailures(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")
	if _, err := openStore(ctx, path, Config{}, func(string, string) (*sql.DB, error) { return nil, injected }); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if _, err := openStore(ctx, path, Config{}, func(string, string) (*sql.DB, error) { return sql.Open("sqlite", ":memory:") }); err == nil {
		t.Fatal("non-WAL database accepted")
	}
	for _, tc := range []struct{ op, prefix string }{{"exec", "CREATE TABLE IF NOT EXISTS es_schema"}, {"query", "SELECT version,store_id"}, {"exec", "\nCREATE TABLE es_topics"}, {"exec", "INSERT INTO es_schema"}} {
		_, err := faultOpen(t, &faultPlan{fail: failSQL(tc.op, tc.prefix)})
		if !errors.Is(err, injected) {
			t.Fatalf("%s %q: %v", tc.op, tc.prefix, err)
		}
	}
	s, path := openTest(t, Config{})
	appSQL(t, s, "UPDATE es_schema SET store_id='' WHERE id=1")
	if _, err := Open(ctx, path, Config{}); err == nil {
		t.Fatal("empty identity")
	}
	appSQL(t, s, "DROP TABLE es_schema;CREATE TABLE es_schema(id INTEGER PRIMARY KEY,version TEXT,store_id TEXT);INSERT INTO es_schema VALUES(1,'invalid','id')")
	if _, err := Open(ctx, path, Config{}); err == nil {
		t.Fatal("schema decode failure ignored")
	}
}

type failingCodec struct{}

func (*failingCodec) Serialize(context.Context, es.Event) ([]byte, error) { return nil, injected }
func (*failingCodec) Deserialize(context.Context, []byte, es.Event) error { return injected }

type valueEvent struct{}

func (valueEvent) GetAggregateId() string       { return "id" }
func (valueEvent) GetEventType() string         { return "value" }
func (valueEvent) GetEventCodec() es.EventCodec { return es.JsonEventCodec{} }

func TestAppendValidationAndFailureIsolation(t *testing.T) {
	ctx := context.Background()
	s, p := faultStore(t)
	var nilEvent *event
	var nilCodec *failingCodec
	bad := []es.WriteEvents{write("", "org", nil, "x"), write("id", "", nil, "x"), write("id", "org", version(-1), "x")}
	noTopic := write("id", "org", nil, "x")
	noTopic.Topic = ""
	bad = append(bad, noTopic)
	for _, e := range []es.Event{nil, nilEvent, valueEvent{}, &event{ID: "id", Kind: " "}, &event{ID: "id", Codec: nilCodec}, &event{ID: "id", Org: "wrong"}, &event{ID: "id", Codec: &failingCodec{}}} {
		w := write("id", "org", nil)
		w.Events = []es.Event{e}
		bad = append(bad, w)
	}
	for _, w := range bad {
		if err := s.SaveEvents(ctx, []es.WriteEvents{w}); err == nil {
			t.Fatalf("invalid append: %+v", w)
		}
	}
	w := write("id", "org", nil, "x")
	if err := s.SaveEvents(ctx, []es.WriteEvents{w, w}); err == nil {
		t.Fatal("duplicate streams")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.SaveEvents(canceled, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, tc := range []struct{ op, prefix string }{{"query", "SELECT partitions"}, {"query", "SELECT version,org"}, {"exec", "INSERT INTO es_streams"}, {"exec", "INSERT INTO es_events"}} {
		p.fail = failSQL(tc.op, tc.prefix)
		if err := s.SaveEvents(ctx, []es.WriteEvents{w}); !errors.Is(err, injected) {
			t.Fatal(tc, err)
		}
		p.fail = nil
		if countRows(t, s, "es_events") != 0 || countRows(t, s, "es_streams") != 0 {
			t.Fatal("failed append changed storage")
		}
	}
	p.alter = func(q string, v []driver.Value) {
		if strings.HasPrefix(q, "SELECT partitions") {
			v[0] = int64(0)
		}
	}
	if err := s.SaveEvents(ctx, []es.WriteEvents{w}); err == nil {
		t.Fatal("invalid stored partition count")
	}
	p.alter = nil
	// A validation error inside a shared transaction must poison that transaction.
	if err := s.Transact(ctx, func(tx *Transaction) error { _ = tx.SaveEvents(ctx, []es.WriteEvents{bad[0]}); return nil }); err == nil {
		t.Fatal("ignored validation error committed")
	}
}

func TestReadAndCursorFailures(t *testing.T) {
	ctx := context.Background()
	s, p := faultStore(t)
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("id", "org", nil, "one", "two")}))
	for _, tc := range []struct {
		topic, stream string
		from          int
		limit         *int
	}{{"", "s", 0, nil}, {"topic", "", 0, nil}, {"topic", "s", -1, nil}, {"topic", "s", 0, version(0)}} {
		if _, err := s.GetEvents(ctx, tc.topic, tc.stream, tc.from, tc.limit); err == nil {
			t.Fatal("invalid stream query")
		}
	}
	if _, err := s.GetLatestEvent(ctx, "", "s"); err == nil {
		t.Fatal("invalid latest query")
	}
	for _, tc := range []struct {
		topic string
		p     int
	}{{"", 0}, {"topic", -1}, {"topic", 1}, {"missing", 0}} {
		if _, err := s.GetTailCursor(ctx, tc.topic, tc.p); err == nil {
			t.Fatal("invalid tail scope")
		}
	}
	if _, err := s.GetEventsByTopic(ctx, "topic", -1, ""); err == nil {
		t.Fatal("negative partition")
	}
	for _, tc := range []struct {
		topic string
		p     int
	}{{"", 0}, {"topic", -2}, {"topic", 1}} {
		if _, err := s.GetEventsByOrgs(ctx, tc.topic, tc.p, nil, ""); err == nil {
			t.Fatal("invalid page scope")
		}
	}
	p.fail = failSQL("query", "SELECT position")
	if _, err := s.GetEvents(ctx, "topic", "id", 0, nil); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if _, err := s.GetLatestEvent(ctx, "topic", "id"); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if _, err := s.GetEventsByTopic(ctx, "topic", 0, ""); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	p.fail = failSQL("query", "SELECT COALESCE")
	if _, err := s.GetTailCursor(ctx, "topic", 0); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	p.fail = nil
	p.alter = func(q string, v []driver.Value) {
		if strings.HasPrefix(q, "SELECT COALESCE") {
			v[0] = "bad"
		}
	}
	if _, err := s.GetTailCursor(ctx, "topic", 0); err == nil {
		t.Fatal("tail scan failure")
	}
	p.alter = func(q string, v []driver.Value) {
		if strings.HasPrefix(q, "SELECT position") {
			v[4] = "bad"
		}
	}
	if _, err := s.GetEvents(ctx, "topic", "id", 0, nil); err == nil {
		t.Fatal("event scan failure")
	}
	p.alter = nil
	appSQL(t, s, "UPDATE es_events SET recorded_at='invalid'")
	if _, err := s.GetEvents(ctx, "topic", "id", 0, nil); err == nil {
		t.Fatal("bad timestamp")
	}
	appSQL(t, s, "UPDATE es_events SET recorded_at='2000-01-01T00:00:00Z',event_type='unknown'")
	if _, err := s.GetEvents(ctx, "topic", "id", 0, nil); err == nil {
		t.Fatal("unknown event type")
	}
	appSQL(t, s, "UPDATE es_events SET event_type='test.changed.v1',payload=x'21'")
	if _, err := s.GetEvents(ctx, "topic", "id", 0, nil); err == nil {
		t.Fatal("bad payload")
	}
	appSQL(t, s, "UPDATE es_events SET payload=x'7b7d'")
	calls := 0
	p.fail = func(op, q string) error {
		if op == "next" && strings.HasPrefix(q, "SELECT position") {
			calls++
			if calls == 2 {
				return injected
			}
		}
		return nil
	}
	rows, err := s.GetEvents(ctx, "topic", "id", 0, nil)
	if !errors.Is(err, injected) || len(rows) != 0 {
		t.Fatalf("partial result on iteration error: %d %v", len(rows), err)
	}
	p.fail = nil
	p.fail = failSQL("query", "SELECT cursor")
	if _, err := s.GetSubscriptionCursor(ctx, "topic", "sub", 0); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	p.fail = nil
}

func TestTopicAndTransactionDatabaseFailures(t *testing.T) {
	ctx := context.Background()
	s, p := faultStore(t)
	for _, tc := range []struct{ op, prefix string }{{"exec", "INSERT INTO es_topics"}, {"query", "SELECT partitions"}} {
		p.fail = failSQL(tc.op, tc.prefix)
		if err := s.EnsureTopic(ctx, "test", 1); !errors.Is(err, injected) {
			t.Fatal(err)
		}
		p.fail = nil
	}
	p.alter = func(q string, v []driver.Value) {
		if strings.HasPrefix(q, "SELECT partitions") {
			v[0] = "bad"
		}
	}
	if err := s.EnsureTopic(ctx, "topic", 1); err == nil {
		t.Fatal("topic scan failure")
	}
	p.alter = nil
	must(t, s.Close())
	if _, err := s.GetSubscriptionCursor(ctx, "topic", "s", 0); err == nil {
		t.Fatal("closed store")
	}
	if err := s.SaveEvents(ctx, []es.WriteEvents{write("id", "org", nil, "x")}); err == nil {
		t.Fatal("closed append")
	}
}

func TestBoundedWriterContention(t *testing.T) {
	ctx := context.Background()
	s, path := openTest(t, Config{BusyTimeout: 10_000_000})
	second, err := Open(ctx, path, Config{BusyTimeout: 10_000_000})
	must(t, err)
	defer second.Close()
	must(t, s.Transact(ctx, func(tx *Transaction) error {
		err := second.SaveEvents(ctx, []es.WriteEvents{write("locked", "a", nil, "x")})
		if err == nil {
			t.Fatal("competing writer bypassed transaction lock")
		}
		return nil
	}))
	if countRows(t, s, "es_events") != 0 {
		t.Fatal("contended write leaked")
	}
}

func Example_transactionBoundary() {
	// Transaction exposes no Commit and does not implement es.EventStore. This
	// prevents an AggregateStore from clearing pending events before outer commit.
	var tx *Transaction
	_, isEventStore := any(tx).(es.EventStore)
	fmt.Println(isEventStore)
	// Output: false
}

func TestCanceledWriteCannotCommit(t *testing.T) {
	s, _ := openTest(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	err := s.Transact(ctx, func(tx *Transaction) error {
		if err := tx.SaveEvents(ctx, []es.WriteEvents{write("canceled", "a", nil, "pending")}); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if err == nil {
		t.Fatal("canceled transaction committed")
	}
	if countRows(t, s, "es_events") != 0 {
		t.Fatal("canceled write leaked")
	}
}

func TestWriterWaitHonorsCancellation(t *testing.T) {
	ctx := context.Background()
	first, path := openTest(t, Config{})
	second, err := Open(ctx, path, Config{})
	must(t, err)
	defer second.Close()
	must(t, first.Transact(ctx, func(*Transaction) error {
		waiting, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		started := time.Now()
		err := second.SaveEvents(waiting, []es.WriteEvents{write("waiting", "org", nil, "x")})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lost cancellation: %v", err)
		}
		if time.Since(started) > time.Second {
			t.Fatal("writer ignored context while waiting for lock")
		}
		return nil
	}))
}

func TestFilenameEscaping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events?#.db")
	s, err := Open(context.Background(), path, Config{})
	must(t, err)
	must(t, s.Close())
	if _, err := os.Stat(path); err != nil {
		t.Fatal("filename was interpreted as URI settings", err)
	}
}

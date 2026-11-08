package sqlitestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/traego/es"
)

type event struct {
	ID, Org, Value string
	Codec          es.EventCodec `json:"-"`
	Kind           string        `json:"-"`
}

func (e *event) GetAggregateId() string { return e.ID }
func (e *event) GetOrgId() string       { return e.Org }
func (e *event) GetEventType() string {
	if e.Kind != "" {
		return e.Kind
	}
	return "test.changed.v1"
}
func (e *event) GetEventCodec() es.EventCodec {
	if e.Codec != nil {
		return e.Codec
	}
	return es.JsonEventCodec{}
}

type aggregate struct {
	es.AggregateRoot
	topic     string
	count     int
	prototype es.Event
}

func (a *aggregate) GetAggregateType() string { return "test" }
func (a *aggregate) GetTopic() string         { return a.topic }
func (a *aggregate) GetPartitionCount() int   { return a.count }
func (a *aggregate) RegisterEvents(register func(es.Event)) {
	if a.prototype != nil {
		register(a.prototype)
	} else {
		register(&event{})
	}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func openTest(t *testing.T, cfg Config) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(context.Background(), path, cfg)
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	must(t, s.RegisterAggregate(context.Background(), &aggregate{topic: "topic", count: 1}))
	return s, path
}
func write(stream, org string, version *int, values ...string) es.WriteEvents {
	w := es.WriteEvents{Topic: "topic", StreamName: stream, OrgId: org, ExpectedVersion: version, CorrelationId: "trace"}
	for _, value := range values {
		w.Events = append(w.Events, &event{ID: stream, Value: value})
	}
	return w
}
func version(n int) *int { return &n }
func appSQL(t *testing.T, s *Store, q string, args ...any) {
	t.Helper()
	_, err := s.db.Exec(q, args...)
	must(t, err)
}
func countRows(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	must(t, s.db.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&n))
	return n
}

func TestDurableReplayAndMetadata(t *testing.T) {
	ctx := context.Background()
	s, path := openTest(t, Config{PageSize: 2})
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("one", "a", version(0), "first", "second"), write("two", "b", nil, "other")}))
	tail, err := s.GetTailCursor(ctx, "topic", 0)
	must(t, err)
	must(t, s.SaveSubscriptionCursor(ctx, "topic", "subscriber", 0, tail))
	must(t, s.Close())
	s, err = Open(ctx, path, Config{})
	must(t, err)
	defer s.Close()
	must(t, s.RegisterAggregate(ctx, &aggregate{topic: "topic", count: 1}))
	records, err := s.GetEvents(ctx, "topic", "one", 0, nil)
	must(t, err)
	if len(records) != 2 || records[0].GetVersion() != 1 || records[1].GetEvent().(*event).Value != "second" || records[0].GetCorrelationId() != "trace" || records[0].GetOrgId() != "a" || records[0].GetGlobalPosition().IsZero() {
		t.Fatalf("history: %+v", records)
	}
	max := 1
	limited, err := s.GetEvents(ctx, "topic", "one", 2, &max)
	must(t, err)
	if len(limited) != 1 || limited[0].GetVersion() != 2 {
		t.Fatal("inclusive stream versions")
	}
	latest, err := s.GetLatestEvent(ctx, "topic", "one")
	must(t, err)
	if latest.GetVersion() != 2 {
		t.Fatal("latest")
	}
	absent, err := s.GetLatestEvent(ctx, "topic", "absent")
	must(t, err)
	if absent != nil {
		t.Fatal("absent stream")
	}
	saved, err := s.GetSubscriptionCursor(ctx, "topic", "subscriber", 0)
	must(t, err)
	if saved != tail {
		t.Fatal("cursor did not survive reopen")
	}
	page, err := s.GetEventsByTopic(ctx, "topic", 0, saved)
	must(t, err)
	if len(page.Events) != 0 || page.NextCursor != saved {
		t.Fatal("idle cursor moved")
	}
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("one", "a", version(2), "third")}))
	page, err = s.GetEventsByTopic(ctx, "topic", 0, saved)
	must(t, err)
	if len(page.Events) != 1 || page.Events[0].GetVersion() != 3 {
		t.Fatal("resume")
	}
}

func TestAtomicBatchVersionsAndImmutableStreams(t *testing.T) {
	ctx := context.Background()
	s, _ := openTest(t, Config{})
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("one", "a", version(0), "first")}))
	err := s.SaveEvents(ctx, []es.WriteEvents{write("two", "b", version(0), "second"), write("one", "a", version(0), "stale")})
	if !errors.Is(err, es.ErrOptimisticConcurrency) {
		t.Fatal(err)
	}
	var detail *es.OptimisticConcurrencyErr
	if !errors.As(err, &detail) || detail.ActualVersion != 1 {
		t.Fatal("conflict detail")
	}
	if countRows(t, s, "es_events") != 1 || countRows(t, s, "es_streams") != 1 {
		t.Fatal("partial batch")
	}
	if err := s.SaveEvents(ctx, []es.WriteEvents{write("one", "b", nil, "wrong org")}); err == nil {
		t.Fatal("org changed")
	}
	must(t, s.EnsureTopic(ctx, "partitioned", 3))
	a := write("one", "a", nil, "first")
	a.Topic = "partitioned"
	a.PartitionKey = "x"
	must(t, s.SaveEvents(ctx, []es.WriteEvents{a}))
	initial, _ := es.CalculatePartitionId("x", 3)
	key := "y"
	for i := 0; ; i++ {
		key = fmt.Sprint(i)
		p, _ := es.CalculatePartitionId(key, 3)
		if p != initial {
			break
		}
	}
	a.PartitionKey = key
	if err := s.SaveEvents(ctx, []es.WriteEvents{a}); err == nil {
		t.Fatal("partition changed")
	}
	// A late SQL failure must roll back the first event and stream metadata too.
	appSQL(t, s, "CREATE TRIGGER reject_second BEFORE INSERT ON es_events WHEN NEW.version=2 BEGIN SELECT RAISE(ABORT,'injected'); END")
	if err := s.SaveEvents(ctx, []es.WriteEvents{write("late", "a", version(0), "first", "fail")}); err == nil {
		t.Fatal("SQL failure accepted")
	}
	if countRows(t, s, "es_events") != 2 {
		t.Fatal("failed batch leaked an event")
	}
	appSQL(t, s, "DROP TRIGGER reject_second")
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("one", "a", nil)}))
	must(t, s.SaveEvents(ctx, nil))
	// Database uniqueness includes version, even if SQL bypasses the adapter.
	_, err = s.db.Exec("INSERT INTO es_events(topic,stream,version,org,partition,recorded_at,event_type,payload,correlation) SELECT topic,stream,version,org,partition,recorded_at,event_type,payload,correlation FROM es_events LIMIT 1")
	if err == nil {
		t.Fatal("duplicate event key accepted")
	}
}

func TestCompetingConnections(t *testing.T) {
	ctx := context.Background()
	a, path := openTest(t, Config{})
	b, err := Open(ctx, path, Config{})
	must(t, err)
	defer b.Close()
	start := make(chan struct{})
	var wins, conflicts atomic.Int32
	var group sync.WaitGroup
	for _, s := range []*Store{a, b} {
		group.Add(1)
		go func(s *Store) {
			defer group.Done()
			<-start
			err := s.SaveEvents(ctx, []es.WriteEvents{write("race", "a", version(0), "value")})
			if err == nil {
				wins.Add(1)
			} else if errors.Is(err, es.ErrOptimisticConcurrency) {
				conflicts.Add(1)
			} else {
				t.Error(err)
			}
		}(s)
	}
	close(start)
	group.Wait()
	if wins.Load() != 1 || conflicts.Load() != 1 || countRows(t, a, "es_events") != 1 {
		t.Fatal("concurrency", wins.Load(), conflicts.Load())
	}
}

func TestPagingIsolationTailAndClockIndependence(t *testing.T) {
	ctx := context.Background()
	s, _ := openTest(t, Config{PageSize: 2})
	empty, err := s.GetTailCursor(ctx, "topic", 0)
	must(t, err)
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("a", "org-a", nil, "1", "2", "3"), write("b", "org-b", nil, "4", "5")}))
	appSQL(t, s, "UPDATE es_events SET recorded_at='2000-01-01T00:00:00Z' WHERE position>2")
	cur := empty
	var got []es.Envelope
	for {
		page, err := s.GetEventsByTopic(ctx, "topic", 0, cur)
		must(t, err)
		if len(page.Events) == 0 {
			if page.NextCursor != cur {
				t.Fatal("idle moved")
			}
			break
		}
		got = append(got, page.Events...)
		cur = page.NextCursor
	}
	if len(got) != 5 {
		t.Fatal("lost equal/backwards-time events", len(got))
	}
	page, err := s.GetEventsByOrgs(ctx, "topic", -1, []string{"org-b"}, "")
	must(t, err)
	if len(page.Events) != 2 || page.Events[0].GetOrgId() != "org-b" {
		t.Fatal("world filter")
	}
	page, err = s.GetEventsByOrgs(ctx, "topic", -1, nil, "")
	must(t, err)
	if len(page.Events) != 2 {
		t.Fatal("all partitions")
	}
	must(t, s.EnsureTopic(ctx, "other", 1))
	w := write("a", "org-a", nil, "other topic")
	w.Topic = "other"
	must(t, s.SaveEvents(ctx, []es.WriteEvents{w}))
	tail, err := s.GetTailCursor(ctx, "topic", 0)
	must(t, err)
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("a", "org-a", nil, "after")}))
	page, err = s.GetEventsByTopic(ctx, "topic", 0, tail)
	must(t, err)
	if len(page.Events) != 1 || page.Events[0].GetEvent().(*event).Value != "after" {
		t.Fatal("tail")
	}
}

func TestSharedTransactionCommitRollbackAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	s, _ := openTest(t, Config{})
	appSQL(t, s, "CREATE TABLE application_state (name TEXT PRIMARY KEY, value INTEGER NOT NULL)")
	boundary, err := s.GetTailCursor(ctx, "topic", 0)
	must(t, err)
	failure := errors.New("application rejected")
	for _, mode := range []string{"callback", "conflict", "ignored SQL", "ignored events", "ignored cursor", "query", "panic", "commit"} {
		t.Run(mode, func(t *testing.T) {
			var escaped *Transaction
			run := func() error {
				return s.Transact(ctx, func(tx *Transaction) error {
					escaped = tx
					_, err := tx.ExecContext(ctx, "INSERT INTO application_state VALUES(?,1)", mode)
					if err != nil {
						return err
					}
					if mode == "conflict" {
						return tx.SaveEvents(ctx, []es.WriteEvents{write("new", "a", version(9), "fail")})
					}
					if mode == "ignored events" {
						_ = tx.SaveEvents(ctx, []es.WriteEvents{write("new", "a", version(9), "fail")})
						return nil
					}
					if mode == "ignored SQL" {
						_, _ = tx.ExecContext(ctx, "INSERT INTO missing_table VALUES(1)")
						return nil
					}
					if mode == "ignored cursor" {
						_ = tx.SaveSubscriptionCursor(ctx, "topic", "sub", 0, "bad")
						return nil
					}
					if mode == "query" {
						_, _ = tx.QueryContext(ctx, "SELECT * FROM missing_table")
						return nil
					}
					if mode == "commit" {
						_, err = tx.ExecContext(ctx, "CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); INSERT INTO child VALUES(99)")
						return err
					}
					if err := tx.SaveEvents(ctx, []es.WriteEvents{write(mode, "a", version(0), "provisional")}); err != nil {
						return err
					}
					if err := tx.SaveSubscriptionCursor(ctx, "topic", "sub", 0, boundary); err != nil {
						return err
					}
					if mode == "panic" {
						panic("callback panic")
					}
					return failure
				})
			}
			if mode == "panic" {
				func() {
					defer func() {
						if recover() == nil {
							t.Error("panic swallowed")
						}
					}()
					_ = run()
				}()
			} else if err := run(); err == nil {
				t.Fatal("failed transaction committed")
			}
			if countRows(t, s, "application_state") != 0 || countRows(t, s, "es_events") != 0 || countRows(t, s, "es_cursors") != 0 {
				t.Fatal("partial shared transaction")
			}
			if _, err := escaped.ExecContext(ctx, "SELECT 1"); !errors.Is(err, ErrTransactionClosed) {
				t.Fatal(err)
			}
			if _, err := escaped.QueryContext(ctx, "SELECT 1"); !errors.Is(err, ErrTransactionClosed) {
				t.Fatal(err)
			}
			if err := escaped.SaveEvents(ctx, nil); !errors.Is(err, ErrTransactionClosed) {
				t.Fatal(err)
			}
			if err := escaped.SaveSubscriptionCursor(ctx, "topic", "sub", 0, ""); !errors.Is(err, ErrTransactionClosed) {
				t.Fatal(err)
			}
		})
	}
	must(t, s.Transact(ctx, func(tx *Transaction) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO application_state VALUES('committed',1)"); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, "SELECT value FROM application_state")
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("cannot see application write")
		}
		var value int
		must(t, rows.Scan(&value))
		must(t, rows.Close())
		if err := tx.SaveEvents(ctx, []es.WriteEvents{write("committed", "a", version(0), "yes")}); err != nil {
			return err
		}
		return tx.SaveSubscriptionCursor(ctx, "topic", "sub", 0, boundary)
	}))
	if countRows(t, s, "application_state") != 1 || countRows(t, s, "es_events") != 1 || countRows(t, s, "es_cursors") != 1 {
		t.Fatal("shared commit missing rows")
	}
}

func TestCheckpointPoliciesAndCursorErrors(t *testing.T) {
	ctx := context.Background()
	s, path := openTest(t, Config{})
	if _, err := s.GetSubscriptionCursor(ctx, "topic", "sub", 0); !errors.Is(err, es.ErrCursorNotFound) {
		t.Fatal(err)
	}
	tail, err := s.GetTailCursor(ctx, "topic", 0)
	must(t, err)
	must(t, s.SaveSubscriptionCursor(ctx, "topic", "sub", 0, tail))
	must(t, s.Close())
	s, err = Open(ctx, path, Config{})
	must(t, err)
	defer s.Close()
	saved, err := s.GetSubscriptionCursor(ctx, "topic", "sub", 0)
	must(t, err)
	if saved != tail {
		t.Fatal("empty-tail checkpoint lost")
	}
	must(t, s.SaveSubscriptionCursor(ctx, "topic", "sub", 0, ""))
	saved, err = s.GetSubscriptionCursor(ctx, "topic", "sub", 0)
	must(t, err)
	if saved != "" {
		t.Fatal("explicit reset")
	}
	other, _ := openTest(t, Config{})
	foreign, err := other.GetTailCursor(ctx, "topic", 0)
	must(t, err)
	for _, value := range []string{"!", "eA", foreign, s.encode("wrong", 0, 0), s.encode("topic", 1, 0), s.encode("topic", 0, -1)} {
		if _, err := s.GetEventsByTopic(ctx, "topic", 0, value); !errors.Is(err, es.ErrCursorNotFound) {
			t.Fatal("bad cursor", err)
		}
		if err := s.SaveSubscriptionCursor(ctx, "topic", "sub", 0, value); !errors.Is(err, es.ErrCursorNotFound) {
			t.Fatal("bad saved cursor", err)
		}
	}
	for _, args := range []struct {
		topic, sub string
		partition  int
	}{{"", "s", 0}, {"topic", "", 0}, {"topic", "s", -1}} {
		if _, err := s.GetSubscriptionCursor(ctx, args.topic, args.sub, args.partition); err == nil {
			t.Fatal("identity")
		}
		if err := s.SaveSubscriptionCursor(ctx, args.topic, args.sub, args.partition, ""); err == nil {
			t.Fatal("identity")
		}
	}
}

func TestCrashHelper(t *testing.T) {
	path := os.Getenv("ES_SQLITE_CRASH_PATH")
	if path == "" {
		return
	}
	s, err := Open(context.Background(), path, Config{})
	if err != nil {
		panic(err)
	}
	if err := s.EnsureTopic(context.Background(), "topic", 1); err != nil {
		panic(err)
	}
	if os.Getenv("ES_SQLITE_CRASH_UNCOMMITTED") == "1" {
		_ = s.Transact(context.Background(), func(tx *Transaction) error {
			if err := tx.SaveEvents(context.Background(), []es.WriteEvents{write("crash", "a", nil, "pending")}); err != nil {
				panic(err)
			}
			os.Exit(0)
			return nil
		})
	} else {
		if err := s.SaveEvents(context.Background(), []es.WriteEvents{write("crash", "a", nil, "committed")}); err != nil {
			panic(err)
		}
		os.Exit(0)
	}
}

func TestProcessCrashRecovery(t *testing.T) {
	for _, pending := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "crash.db")
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
		cmd.Env = append(os.Environ(), "ES_SQLITE_CRASH_PATH="+path, fmt.Sprintf("ES_SQLITE_CRASH_UNCOMMITTED=%d", map[bool]int{false: 0, true: 1}[pending]))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v %s", err, out)
		}
		s, err := Open(context.Background(), path, Config{})
		must(t, err)
		want := 1
		if pending {
			want = 0
		}
		if countRows(t, s, "es_events") != want {
			t.Fatal("crash recovery", pending)
		}
		must(t, s.Close())
	}
}

func TestConfigurationCancellationAndSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")
	for _, tc := range []struct {
		path string
		cfg  Config
	}{{"relative.db", Config{}}, {path, Config{PageSize: -1}}, {path, Config{BusyTimeout: -1}}, {path, Config{MaxOpenConns: -1}}, {path, Config{BusyTimeout: time.Nanosecond}}} {
		if s, err := Open(ctx, tc.path, tc.cfg); err == nil {
			s.Close()
			t.Fatal("invalid config")
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Open(canceled, path, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := Open(ctx, filepath.Join(t.TempDir(), "missing", "events.db"), Config{}); err == nil {
		t.Fatal("bad parent")
	}
	s, _ := openTest(t, Config{MaxOpenConns: 1, BusyTimeout: time.Millisecond})
	for name, want := range map[string]string{"journal_mode": "wal", "synchronous": "2", "foreign_keys": "1"} {
		var got string
		must(t, s.db.QueryRow("PRAGMA "+name).Scan(&got))
		if got != want {
			t.Fatal(name, got)
		}
	}
	must(t, s.EnsureTopic(ctx, "topic", 1))
	if err := s.EnsureTopic(ctx, "topic", 2); err == nil {
		t.Fatal("topology changed")
	}
	for _, count := range []int{0, -1, int(1 << 32)} {
		if err := s.EnsureTopic(ctx, "x", count); err == nil {
			t.Fatal("bad topology")
		}
	}
	if err := s.EnsureTopic(ctx, "", 1); err == nil {
		t.Fatal("bad topic")
	}
	if err := s.RegisterAggregate(ctx, nil); err == nil {
		t.Fatal("nil aggregate")
	}
	if err := s.RegisterAggregate(ctx, &aggregate{topic: "topic", count: 2}); err == nil {
		t.Fatal("registry bypassed topology")
	}
	appSQL(t, s, "UPDATE es_schema SET version=2")
	if _, err := Open(ctx, sPath(t, s), Config{}); err == nil {
		t.Fatal("future schema accepted")
	}
	must(t, s.Close())
	if err := s.Transact(ctx, func(*Transaction) error { return nil }); err == nil {
		t.Fatal("closed database")
	}
	if err := s.Transact(ctx, nil); err == nil {
		t.Fatal("nil callback")
	}
}
func sPath(t *testing.T, s *Store) string {
	t.Helper()
	var seq int
	var name, path string
	must(t, s.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path))
	return path
}

package sqlitestore

import (
	"context"
	"errors"
	"fmt"
	"github.com/traego/es"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func projection() MaterializerConfig {
	return MaterializerConfig{Topic: "topic", SubscriberID: "counts-v1", Partition: 0}
}
func referenceView(t *testing.T, s *Store) {
	t.Helper()
	appSQL(t, s, `CREATE TABLE reference_counts(org TEXT,stream TEXT,total INTEGER,version INTEGER,PRIMARY KEY(org,stream)) STRICT`)
}
func countEvent(ctx context.Context, tx *Transaction, e es.Envelope) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO reference_counts VALUES(?,?,1,?) ON CONFLICT(org,stream) DO UPDATE SET total=reference_counts.total+1,version=excluded.version WHERE reference_counts.version<excluded.version`, e.GetOrgId(), e.GetStreamName(), e.GetVersion())
	return err
}
func referenceRows(t *testing.T, s *Store) string {
	t.Helper()
	rows, err := s.db.Query("SELECT org,stream,total,version FROM reference_counts ORDER BY org,stream")
	must(t, err)
	defer rows.Close()
	result := ""
	for rows.Next() {
		var org, stream string
		var total, version int
		must(t, rows.Scan(&org, &stream, &total, &version))
		result += fmt.Sprintf("%s/%s:%d@%d;", org, stream, total, version)
	}
	must(t, rows.Err())
	return result
}
func drain(t *testing.T, s *Store, cfg MaterializerConfig) int {
	t.Helper()
	total := 0
	for {
		r, err := s.MaterializeBatch(materializerContext(t), cfg, countEvent)
		must(t, err)
		total += r.Processed
		if r.Processed == 0 {
			return total
		}
	}
}

func TestMaterializerReferenceRestartAndRebuild(t *testing.T) {
	ctx := materializerContext(t)
	s, path := openTest(t, Config{PageSize: 2, MaxOpenConns: 1})
	referenceView(t, s)
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("a", "home", nil, "1", "2", "3"), write("b", "home", nil, "4", "5")}))
	first, err := s.MaterializeBatch(ctx, projection(), countEvent)
	must(t, err)
	if first.Processed != 2 || first.Cursor == "" || referenceRows(t, s) != "home/a:2@2;" {
		t.Fatal("view read ahead of its checkpoint", first, referenceRows(t, s))
	}
	saved, err := s.GetSubscriptionCursor(ctx, "topic", "counts-v1", 0)
	must(t, err)
	if saved != first.Cursor {
		t.Fatal("checkpoint and view differ")
	}
	must(t, s.Close())
	s, err = Open(ctx, path, Config{PageSize: 2, MaxOpenConns: 1})
	must(t, err)
	defer s.Close()
	must(t, s.RegisterAggregate(ctx, &aggregate{topic: "topic", count: 1}))
	cfg := projection()
	cfg.StartPosition = es.StartTail
	if drain(t, s, cfg) != 3 {
		t.Fatal("restart skipped or repeated events")
	}
	before := referenceRows(t, s)
	if before != "home/a:3@3;home/b:2@2;" {
		t.Fatal(before)
	}
	must(t, s.Transact(ctx, func(tx *Transaction) error {
		if _, err := tx.ExecContext(ctx, "DELETE FROM reference_counts"); err != nil {
			return err
		}
		return tx.SaveSubscriptionCursor(ctx, "topic", "counts-v1", 0, "")
	}))
	if drain(t, s, cfg) != 5 || referenceRows(t, s) != before {
		t.Fatal("rebuild differs", referenceRows(t, s))
	}
}

func TestMaterializerRollbackAndRetry(t *testing.T) {
	ctx := materializerContext(t)
	s, _ := openTest(t, Config{PageSize: 2, MaxOpenConns: 1})
	referenceView(t, s)
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("a", "home", nil, "1", "2", "3", "4")}))
	first, err := s.MaterializeBatch(ctx, projection(), countEvent)
	must(t, err)
	before := referenceRows(t, s)
	for _, mode := range []string{"handler", "ignored SQL", "checkpoint", "panic", "cancel", "commit"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if mode == "checkpoint" {
				appSQL(t, s, "CREATE TRIGGER reject_checkpoint BEFORE UPDATE ON es_cursors BEGIN SELECT RAISE(ABORT,'failed');END")
				defer appSQL(t, s, "DROP TRIGGER reject_checkpoint")
			}
			run := func() (MaterializationResult, error) {
				return s.MaterializeBatch(runCtx, projection(), func(ctx context.Context, tx *Transaction, e es.Envelope) error {
					calls++
					if err := countEvent(ctx, tx, e); err != nil {
						return err
					}
					if mode == "commit" && calls == 1 {
						_, err := tx.ExecContext(ctx, "CREATE TABLE parent(id INTEGER PRIMARY KEY);CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);INSERT INTO child VALUES(99)")
						return err
					}
					if mode == "ignored SQL" {
						_, _ = tx.ExecContext(ctx, "INSERT INTO missing_view VALUES(1)")
						return nil
					}
					if mode == "cancel" {
						cancel()
						return ctx.Err()
					}
					if calls == 2 {
						if mode == "handler" {
							return injected
						}
						if mode == "panic" {
							panic("handler panic")
						}
					}
					return nil
				})
			}
			if mode == "panic" {
				func() {
					defer func() {
						if recover() == nil {
							t.Error("panic swallowed")
						}
					}()
					_, _ = run()
				}()
			} else {
				r, err := run()
				if err == nil || r.Processed != 0 || r.Cursor != "" {
					t.Fatal("failed page acknowledged", r, err)
				}
				if mode == "handler" && !errors.Is(err, injected) {
					t.Fatal(err)
				}
			}
			if mode == "ignored SQL" && calls != 1 {
				t.Fatal("continued after rollback-only failure")
			}
			if referenceRows(t, s) != before {
				t.Fatal("partial view committed")
			}
			saved, err := s.GetSubscriptionCursor(ctx, "topic", "counts-v1", 0)
			must(t, err)
			if saved != first.Cursor {
				t.Fatal("checkpoint advanced on failure")
			}
		})
	}
	if drain(t, s, projection()) != 2 || referenceRows(t, s) != "home/a:4@4;" {
		t.Fatal("retry lost or duplicated events")
	}
}

func TestMaterializerInitialPositions(t *testing.T) {
	ctx := materializerContext(t)
	for _, pos := range []es.StartPosition{es.StartBeginning, es.StartTail} {
		s, _ := openTest(t, Config{MaxOpenConns: 1})
		referenceView(t, s)
		must(t, s.SaveEvents(ctx, []es.WriteEvents{write("a", "home", nil, "old")}))
		cfg := projection()
		cfg.StartPosition = pos
		r, err := s.MaterializeBatch(ctx, cfg, countEvent)
		must(t, err)
		want := 1
		if pos == es.StartTail {
			want = 0
		}
		if r.Processed != want {
			t.Fatal("start policy", r)
		}
		saved, err := s.GetSubscriptionCursor(ctx, "topic", "counts-v1", 0)
		must(t, err)
		if saved != r.Cursor {
			t.Fatal("initial checkpoint missing")
		}
		must(t, s.SaveEvents(ctx, []es.WriteEvents{write("a", "home", nil, "new")}))
		cfg.StartPosition = es.StartTail
		r, err = s.MaterializeBatch(ctx, cfg, countEvent)
		must(t, err)
		if r.Processed != 1 {
			t.Fatal("saved checkpoint ignored")
		}
	}
	s, path := openTest(t, Config{MaxOpenConns: 1})
	referenceView(t, s)
	cfg := projection()
	cfg.StartPosition = es.StartTail
	r, err := s.MaterializeBatch(ctx, cfg, countEvent)
	must(t, err)
	if r.Processed != 0 {
		t.Fatal(r)
	}
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("a", "home", nil, "new")}))
	must(t, s.Close())
	s, err = Open(ctx, path, Config{MaxOpenConns: 1})
	must(t, err)
	defer s.Close()
	must(t, s.RegisterAggregate(ctx, &aggregate{topic: "topic", count: 1}))
	r, err = s.MaterializeBatch(ctx, cfg, countEvent)
	must(t, err)
	if r.Processed != 1 {
		t.Fatal("empty tail recaptured")
	}
}

func TestMaterializerInitialFailure(t *testing.T) {
	ctx := materializerContext(t)
	s, _ := openTest(t, Config{MaxOpenConns: 1})
	referenceView(t, s)
	must(t, s.SaveEvents(ctx, []es.WriteEvents{write("a", "home", nil, "1")}))
	_, err := s.MaterializeBatch(ctx, projection(), func(ctx context.Context, tx *Transaction, e es.Envelope) error {
		_ = countEvent(ctx, tx, e)
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if countRows(t, s, "reference_counts") != 0 {
		t.Fatal("initial view leaked")
	}
	if _, err := s.GetSubscriptionCursor(ctx, "topic", "counts-v1", 0); !errors.Is(err, es.ErrCursorNotFound) {
		t.Fatal("failed initial checkpoint committed", err)
	}
}

func TestMaterializerPartitionAndSubscriberIsolation(t *testing.T) {
	ctx := materializerContext(t)
	s, _ := openTest(t, Config{MaxOpenConns: 1})
	referenceView(t, s)
	must(t, s.EnsureTopic(ctx, "partitioned", 2))
	for p := 0; p < 2; p++ {
		key := ""
		for i := 0; ; i++ {
			key = fmt.Sprint(i)
			got, _ := es.CalculatePartitionId(key, 2)
			if got == p {
				break
			}
		}
		w := write(fmt.Sprint(p), "home", nil, "x")
		w.Topic = "partitioned"
		w.PartitionKey = key
		must(t, s.SaveEvents(ctx, []es.WriteEvents{w}))
	}
	for p := 0; p < 2; p++ {
		cfg := projection()
		cfg.Topic = "partitioned"
		cfg.Partition = p
		r, err := s.MaterializeBatch(ctx, cfg, countEvent)
		must(t, err)
		if r.Processed != 1 {
			t.Fatal("partition isolation", r)
		}
	}
	cfg := projection()
	cfg.Topic = "partitioned"
	cfg.SubscriberID = "other-view"
	r, err := s.MaterializeBatch(ctx, cfg, func(context.Context, *Transaction, es.Envelope) error { return nil })
	must(t, err)
	if r.Processed != 1 {
		t.Fatal("subscriber isolation")
	}
}

func TestMaterializerConcurrentWorkers(t *testing.T) {
	ctx := materializerContext(t)
	a, path := openTest(t, Config{PageSize: 1, MaxOpenConns: 1})
	referenceView(t, a)
	must(t, a.SaveEvents(ctx, []es.WriteEvents{write("a", "home", nil, "1", "2")}))
	b, err := Open(ctx, path, Config{PageSize: 1, MaxOpenConns: 1})
	must(t, err)
	defer b.Close()
	must(t, b.RegisterAggregate(ctx, &aggregate{topic: "topic", count: 1}))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, s := range []*Store{a, b} {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			<-start
			r, err := s.MaterializeBatch(ctx, projection(), countEvent)
			if err != nil {
				t.Error(err)
			} else if r.Processed != 1 {
				t.Error("workers reused checkpoint", r)
			}
		}(s)
	}
	close(start)
	wg.Wait()
	if referenceRows(t, a) != "home/a:2@2;" {
		t.Fatal("duplicate worker updates", referenceRows(t, a))
	}
}

func TestMaterializerFailures(t *testing.T) {
	ctx := materializerContext(t)
	s, p := faultStore(t)
	referenceView(t, s)
	for _, change := range []func(*MaterializerConfig){func(c *MaterializerConfig) { c.Topic = "" }, func(c *MaterializerConfig) { c.SubscriberID = "" }, func(c *MaterializerConfig) { c.Partition = -1 }, func(c *MaterializerConfig) { c.Partition = 1 }, func(c *MaterializerConfig) { c.StartPosition = es.StartPosition(9) }} {
		cfg := projection()
		change(&cfg)
		if _, err := s.MaterializeBatch(ctx, cfg, countEvent); err == nil {
			t.Fatal("invalid config")
		}
	}
	if _, err := s.MaterializeBatch(ctx, projection(), nil); err == nil {
		t.Fatal("nil handler")
	}
	for _, prefix := range []string{"SELECT cursor", "SELECT partitions", "SELECT position"} {
		p.fail = failSQL("query", prefix)
		_, err := s.MaterializeBatch(ctx, projection(), countEvent)
		if !errors.Is(err, injected) {
			t.Fatal(prefix, err)
		}
		p.fail = nil
	}
	p.fail = failSQL("exec", "INSERT INTO es_cursors")
	if _, err := s.MaterializeBatch(ctx, projection(), countEvent); !errors.Is(err, injected) {
		t.Fatal("initial checkpoint", err)
	}
	p.fail = nil
	cfg := projection()
	cfg.StartPosition = es.StartTail
	p.fail = failSQL("query", "SELECT COALESCE")
	if _, err := s.MaterializeBatch(ctx, cfg, countEvent); !errors.Is(err, injected) {
		t.Fatal("tail failure", err)
	}
	p.fail = nil
	appSQL(t, s, "INSERT INTO es_cursors VALUES('topic','counts-v1',0,'malformed')")
	if _, err := s.MaterializeBatch(ctx, projection(), countEvent); !errors.Is(err, es.ErrCursorNotFound) {
		t.Fatal("corrupt checkpoint reset", err)
	}
	saved, err := s.GetSubscriptionCursor(ctx, "topic", "counts-v1", 0)
	must(t, err)
	if saved != "malformed" {
		t.Fatal("corrupt cursor replaced")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.MaterializeBatch(canceled, projection(), countEvent); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func materializerContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestMaterializerCrashHelper(t *testing.T) {
	path := os.Getenv("ES_SQLITE_PROJECTION_CRASH_PATH")
	if path == "" {
		return
	}
	ctx := context.Background()
	s, err := Open(ctx, path, Config{PageSize: 2, MaxOpenConns: 1})
	if err != nil {
		panic(err)
	}
	if err := s.RegisterAggregate(ctx, &aggregate{topic: "topic", count: 1}); err != nil {
		panic(err)
	}
	_, err = s.MaterializeBatch(ctx, projection(), func(ctx context.Context, tx *Transaction, e es.Envelope) error {
		if err := countEvent(ctx, tx, e); err != nil {
			return err
		}
		if os.Getenv("ES_SQLITE_PROJECTION_CRASH_PENDING") == "1" {
			os.Exit(0)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	os.Exit(0)
}

func TestMaterializerProcessCrashRecovery(t *testing.T) {
	for _, pending := range []bool{true, false} {
		ctx := materializerContext(t)
		s, path := openTest(t, Config{PageSize: 2, MaxOpenConns: 1})
		referenceView(t, s)
		must(t, s.SaveEvents(ctx, []es.WriteEvents{write("a", "home", nil, "1", "2", "3", "4")}))
		first, err := s.MaterializeBatch(ctx, projection(), countEvent)
		must(t, err)
		flag := "0"
		if pending {
			flag = "1"
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMaterializerCrashHelper$")
		cmd.Env = append(os.Environ(), "ES_SQLITE_PROJECTION_CRASH_PATH="+path, "ES_SQLITE_PROJECTION_CRASH_PENDING="+flag)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v %s", err, out)
		}
		must(t, s.Close())
		s, err = Open(ctx, path, Config{PageSize: 2, MaxOpenConns: 1})
		must(t, err)
		must(t, s.RegisterAggregate(ctx, &aggregate{topic: "topic", count: 1}))
		saved, err := s.GetSubscriptionCursor(ctx, "topic", "counts-v1", 0)
		must(t, err)
		if pending {
			if referenceRows(t, s) != "home/a:2@2;" || saved != first.Cursor {
				t.Fatal("uncommitted projection/checkpoint survived crash")
			}
			if drain(t, s, projection()) != 2 {
				t.Fatal("failed page not retried")
			}
		} else if drain(t, s, projection()) != 0 {
			t.Fatal("committed page repeated after crash")
		}
		if referenceRows(t, s) != "home/a:4@4;" {
			t.Fatal("crash recovery produced the wrong view", referenceRows(t, s))
		}
		must(t, s.Close())
	}
}

// Package sqlitestore persists es events and subscription checkpoints in SQLite.
// The engine uses es.EventStore; SQLite-specific application transactions remain
// in this adapter. A store owns its database pool and must be closed by its owner.
package sqlitestore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/traego/es"
	_ "modernc.org/sqlite"
)

// Config controls paging and bounded SQLite lock contention. Zero values select
// a page size of 50, a five-second busy timeout and four pooled connections.
type Config struct {
	// PageSize limits each topic page; zero selects 50.
	PageSize int
	// BusyTimeout bounds SQLite lock contention; zero selects five seconds.
	BusyTimeout time.Duration
	// MaxOpenConns bounds the connection pool; zero selects four.
	MaxOpenConns int
}

// Store is an es.EventStore backed by a local SQLite database. All connections
// use immediate write transactions, foreign keys and FULL synchronous mode.
type Store struct {
	db       *sql.DB
	registry *es.EventRegistry
	pageSize int
	id       string
}

var _ es.EventStore = (*Store)(nil)

// Open opens an absolute local filename, enables WAL and applies es-owned schema
// migrations atomically. It never falls back to memory. Application tables can
// share this file; names beginning with es_ are reserved for this adapter.
func Open(ctx context.Context, path string, cfg Config) (*Store, error) {
	return openStore(ctx, path, cfg, sql.Open)
}

func openStore(ctx context.Context, path string, cfg Config, opener func(string, string) (*sql.DB, error)) (*Store, error) {
	if !filepath.IsAbs(path) || cfg.PageSize < 0 || cfg.BusyTimeout < 0 || cfg.MaxOpenConns < 0 {
		return nil, fmt.Errorf("absolute database path and nonnegative settings are required")
	}
	if cfg.PageSize == 0 {
		cfg.PageSize = 50
	}
	if cfg.BusyTimeout == 0 {
		cfg.BusyTimeout = 5 * time.Second
	}
	if cfg.MaxOpenConns == 0 {
		cfg.MaxOpenConns = 4
	}
	if cfg.BusyTimeout < time.Millisecond {
		return nil, fmt.Errorf("busy timeout must be at least one millisecond")
	}
	u := url.URL{Scheme: "file", Path: path}
	q := url.Values{"_txlock": {"immediate"}, "_pragma": {"foreign_keys(1)", "synchronous(FULL)", fmt.Sprintf("busy_timeout(%d)", cfg.BusyTimeout.Milliseconds())}}
	u.RawQuery = q.Encode()
	db, err := opener("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxOpenConns)
	s := &Store{db: db, registry: es.NewEventRegistry(), pageSize: cfg.PageSize}
	if err := s.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) initialize(ctx context.Context) error {
	var mode string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		return fmt.Errorf("enable WAL: %w", err)
	}
	if mode != "wal" {
		return fmt.Errorf("SQLite WAL required, got %q", mode)
	}
	return s.Transact(ctx, func(tx *Transaction) error {
		if _, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS es_schema (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, store_id TEXT NOT NULL) STRICT"); err != nil {
			return err
		}
		var version int
		err := tx.sql.QueryRowContext(ctx, "SELECT version,store_id FROM es_schema WHERE id=1").Scan(&version, &s.id)
		if errors.Is(err, sql.ErrNoRows) {
			if _, err := tx.ExecContext(ctx, schema); err != nil {
				return fmt.Errorf("create event schema: %w", err)
			}
			s.id = rand.Text()
			_, err := tx.ExecContext(ctx, "INSERT INTO es_schema(id,version,store_id) VALUES(1,1,?)", s.id)
			return err
		}
		if err != nil {
			return fmt.Errorf("read event schema version: %w", err)
		}
		if version != 1 || s.id == "" {
			return fmt.Errorf("unsupported or invalid event schema version %d", version)
		}
		return nil
	})
}

// Close releases this store's pool. Stop users of the store before closing it.
func (s *Store) Close() error { return s.db.Close() }

func valid(value string) bool { return strings.TrimSpace(value) != "" }

// EnsureTopic creates a topic or verifies its immutable partition count.
func (s *Store) EnsureTopic(ctx context.Context, topic string, count int) error {
	if !valid(topic) || count <= 0 || uint64(count) > 1<<32-1 {
		return fmt.Errorf("topic and valid partition count are required")
	}
	return s.Transact(ctx, func(tx *Transaction) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO es_topics(topic,partitions) VALUES(?,?) ON CONFLICT(topic) DO NOTHING", topic, count)
		if err != nil {
			return err
		}
		var current int
		if err := tx.sql.QueryRowContext(ctx, "SELECT partitions FROM es_topics WHERE topic=?", topic).Scan(&current); err != nil {
			return err
		}
		if current != count {
			return fmt.Errorf("topic partition count cannot change from %d to %d", current, count)
		}
		return nil
	})
}

// RegisterAggregate registers stable event codecs and ensures its topic.
func (s *Store) RegisterAggregate(ctx context.Context, aggregate es.AggregateRoot) error {
	if err := s.registry.RegisterAggregate(ctx, aggregate); err != nil {
		return err
	}
	return s.EnsureTopic(ctx, aggregate.GetTopic(), aggregate.GetPartitionCount())
}

const schema = `
CREATE TABLE es_topics (topic TEXT PRIMARY KEY, partitions INTEGER NOT NULL CHECK(partitions>0)) STRICT;
CREATE TABLE es_streams (topic TEXT NOT NULL,stream TEXT NOT NULL,org TEXT NOT NULL,partition INTEGER NOT NULL,version INTEGER NOT NULL CHECK(version>0),PRIMARY KEY(topic,stream),FOREIGN KEY(topic) REFERENCES es_topics(topic)) STRICT;
CREATE TABLE es_events (position INTEGER PRIMARY KEY AUTOINCREMENT,topic TEXT NOT NULL,stream TEXT NOT NULL,version INTEGER NOT NULL CHECK(version>0),org TEXT NOT NULL,partition INTEGER NOT NULL,recorded_at TEXT NOT NULL,event_type TEXT NOT NULL,payload BLOB NOT NULL,correlation TEXT NOT NULL,UNIQUE(topic,stream,version),FOREIGN KEY(topic,stream) REFERENCES es_streams(topic,stream)) STRICT;
CREATE INDEX es_events_topic ON es_events(topic,partition,position);
CREATE INDEX es_events_org ON es_events(topic,org,partition,position);
CREATE TABLE es_cursors (topic TEXT NOT NULL,subscriber TEXT NOT NULL,partition INTEGER NOT NULL,cursor TEXT NOT NULL,PRIMARY KEY(topic,subscriber,partition)) STRICT;
`

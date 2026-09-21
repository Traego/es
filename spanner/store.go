// Package spannerstore persists github.com/traego/es events in Google Cloud
// Spanner. Streams are identified by (topic, stream name), and batches commit
// atomically with optimistic concurrency checks in the same transaction.
package spannerstore

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/traego/es"
)

// Config controls page size. Zero selects the default of 50.
type Config struct{ PageSize int }

// Store implements es.EventStore. The caller owns and closes the Spanner client.
type Store struct {
	db       backend
	registry *es.EventRegistry
	pageSize int
}

var _ es.EventStore = (*Store)(nil)

// New constructs a store. Use CreateSchema once and EnsureTopic (or
// RegisterAggregate) before writing. Existing topic partition counts are fixed.
func New(client *spanner.Client, cfg Config) (*Store, error) {
	if client == nil || cfg.PageSize < 0 {
		return nil, fmt.Errorf("client is required and page size must not be negative")
	}
	if cfg.PageSize == 0 {
		cfg.PageSize = 50
	}
	return &Store{db: clientBackend{client}, registry: es.NewEventRegistry(), pageSize: cfg.PageSize}, nil
}

func nonempty(value string) bool { return strings.TrimSpace(value) != "" }

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

// EnsureTopic creates a topic or verifies its existing partition count. Online
// repartitioning needs a separate migration protocol and is deliberately refused.
func (s *Store) EnsureTopic(ctx context.Context, topic string, count int) error {
	if !nonempty(topic) || count <= 0 || uint64(count) > 1<<32-1 {
		return fmt.Errorf("topic and partition count between 1 and 4294967295 are required")
	}
	return s.db.transact(ctx, func(tx transaction) error {
		rows, err := tx.read(ctx, spanner.Statement{SQL: "SELECT partition_count FROM Topics WHERE topic_name = @topic", Params: map[string]any{"topic": topic}})
		if err != nil {
			return fmt.Errorf("read topic: %w", err)
		}
		if len(rows) == 0 {
			return tx.buffer([]*spanner.Mutation{spanner.Insert("Topics", []string{"topic_name", "partition_count"}, []any{topic, int64(count)})})
		}
		var existing int64
		if err := rows[0].Column(0, &existing); err != nil {
			return fmt.Errorf("decode topic: %w", err)
		}
		if existing != int64(count) {
			return fmt.Errorf("topic %q has %d partitions; cannot change to %d", topic, existing, count)
		}
		return nil
	})
}

// RegisterAggregate ensures the topic and registers codecs for historic reads.
func (s *Store) RegisterAggregate(ctx context.Context, aggregate es.AggregateRoot) error {
	if isNil(aggregate) {
		return fmt.Errorf("aggregate is required")
	}
	if err := s.registry.RegisterAggregate(ctx, aggregate); err != nil {
		return err
	}
	return s.EnsureTopic(ctx, aggregate.GetTopic(), aggregate.GetPartitionCount())
}

type preparedWrite struct {
	es.WriteEvents
	data  [][]byte
	types []string
}

// SaveEvents serializes and validates the entire batch before opening a
// transaction. Event rows use insert-only mutations and can never be overwritten.
// OrgId is authoritative; optional event tenant metadata must agree when set.
func (s *Store) SaveEvents(ctx context.Context, writes []es.WriteEvents) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared := make([]preparedWrite, 0, len(writes))
	seen := make(map[[2]string]bool)
	for _, write := range writes {
		if !nonempty(write.Topic) || !nonempty(write.StreamName) || !nonempty(write.OrgId) {
			return fmt.Errorf("topic, stream name, and org ID are required")
		}
		if write.ExpectedVersion != nil && *write.ExpectedVersion < 0 {
			return fmt.Errorf("expected version must not be negative")
		}
		key := [2]string{write.Topic, write.StreamName}
		if seen[key] {
			return fmt.Errorf("duplicate topic/stream: %q/%q", key[0], key[1])
		}
		seen[key] = true
		if write.PartitionKey == "" {
			write.PartitionKey = write.StreamName
		}
		p := preparedWrite{WriteEvents: write}
		for _, event := range write.Events {
			if isNil(event) || !nonempty(event.GetEventType()) || isNil(event.GetEventCodec()) {
				return fmt.Errorf("event, event type, and codec are required")
			}
			if orgEvent, ok := event.(es.OrgEvent); ok && orgEvent.GetOrgId() != "" && orgEvent.GetOrgId() != write.OrgId {
				return fmt.Errorf("event tenant does not match write tenant")
			}
			data, err := event.GetEventCodec().Serialize(ctx, event)
			if err != nil {
				return fmt.Errorf("serialize event: %w", err)
			}
			// A codec may reuse its scratch buffer on the next serialization.
			p.data = append(p.data, append([]byte(nil), data...))
			p.types = append(p.types, event.GetEventType())
		}
		prepared = append(prepared, p)
	}
	if len(prepared) == 0 {
		return nil
	}
	return s.db.transact(ctx, func(tx transaction) error {
		var mutations []*spanner.Mutation
		for _, write := range prepared {
			rows, err := tx.read(ctx, spanner.Statement{SQL: "SELECT partition_count FROM Topics WHERE topic_name = @topic", Params: map[string]any{"topic": write.Topic}})
			if err != nil {
				return fmt.Errorf("read topic: %w", err)
			}
			if len(rows) == 0 {
				return fmt.Errorf("topic %q must be ensured before writing", write.Topic)
			}
			var count int64
			if err := rows[0].Column(0, &count); err != nil {
				return fmt.Errorf("decode partition count: %w", err)
			}
			partition, err := es.CalculatePartitionId(write.PartitionKey, int(count))
			if err != nil {
				return err
			}
			rows, err = tx.read(ctx, spanner.Statement{SQL: "SELECT version, org_id, partition_id FROM Events WHERE topic = @topic AND stream_name = @stream ORDER BY version DESC LIMIT 1", Params: map[string]any{"topic": write.Topic, "stream": write.StreamName}})
			if err != nil {
				return fmt.Errorf("read stream version: %w", err)
			}
			var latest, existingPartition int64
			var existingOrg string
			if len(rows) > 1 {
				return fmt.Errorf("stream version query returned %d rows", len(rows))
			}
			if len(rows) == 1 {
				if err := rows[0].Columns(&latest, &existingOrg, &existingPartition); err != nil {
					return fmt.Errorf("decode stream metadata: %w", err)
				}
			}
			version := int(latest)
			if write.ExpectedVersion != nil && *write.ExpectedVersion != version {
				return &es.OptimisticConcurrencyErr{StreamName: write.StreamName, ExpectedVersion: *write.ExpectedVersion, ActualVersion: version}
			}
			if len(rows) == 1 && (existingOrg != write.OrgId || existingPartition != int64(partition)) {
				return fmt.Errorf("stream %q tenant and partition are immutable", write.StreamName)
			}
			for i, data := range write.data {
				mutations = append(mutations, spanner.Insert("Events", []string{"topic", "stream_name", "version", "global_timestamp", "partition_id", "org_id", "correlation_id", "event_type", "event_data"}, []any{write.Topic, write.StreamName, int64(version + i + 1), spanner.CommitTimestamp, int64(partition), write.OrgId, write.CorrelationId, write.types[i], data}))
			}
		}
		return tx.buffer(mutations)
	})
}

const eventColumns = "topic, org_id, stream_name, global_timestamp, version, event_type, event_data, correlation_id"

func (s *Store) readEvents(ctx context.Context, stmt spanner.Statement) ([]es.Envelope, error) {
	rows, err := s.db.read(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}
	events := make([]es.Envelope, 0, len(rows))
	for _, row := range rows {
		event, err := s.decodeEvent(ctx, row)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// GetEvents reads a stream in ascending version order, including fromVersion.
func (s *Store) GetEvents(ctx context.Context, topic, stream string, fromVersion int, limit *int) ([]es.Envelope, error) {
	if !nonempty(topic) || !nonempty(stream) || fromVersion < 0 || (limit != nil && *limit <= 0) {
		return nil, fmt.Errorf("valid topic, stream, version, and positive limit are required")
	}
	stmt := spanner.Statement{SQL: "SELECT " + eventColumns + " FROM Events WHERE topic = @topic AND stream_name = @stream AND version >= @version ORDER BY version ASC", Params: map[string]any{"topic": topic, "stream": stream, "version": int64(fromVersion)}}
	if limit != nil {
		stmt.SQL += " LIMIT @limit"
		stmt.Params["limit"] = int64(*limit)
	}
	return s.readEvents(ctx, stmt)
}

// GetLatestEvent returns nil when a stream has no events.
func (s *Store) GetLatestEvent(ctx context.Context, topic, stream string) (*es.Envelope, error) {
	if !nonempty(topic) || !nonempty(stream) {
		return nil, fmt.Errorf("topic and stream are required")
	}
	events, err := s.readEvents(ctx, spanner.Statement{SQL: "SELECT " + eventColumns + " FROM Events WHERE topic = @topic AND stream_name = @stream ORDER BY version DESC LIMIT 1", Params: map[string]any{"topic": topic, "stream": stream}})
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}
	return &events[0], nil
}

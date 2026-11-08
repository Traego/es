package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/traego/es"
)

const columns = "position,topic,org,stream,version,recorded_at,event_type,payload,correlation"

type record struct {
	position int64
	envelope es.Envelope
}

// queryer is implemented by both a database pool and its active transaction.
// Shared read paths must use the supplied executor to preserve one snapshot.
type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) read(ctx context.Context, query string, args ...any) ([]record, error) {
	return s.readFrom(ctx, s.db, query, args...)
}

func (s *Store) readFrom(ctx context.Context, source queryer, query string, args ...any) ([]record, error) {
	rows, err := source.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []record
	for rows.Next() {
		var position int64
		var version int
		var topic, org, stream, stamp, kind, correlation string
		var payload []byte
		if err := rows.Scan(&position, &topic, &org, &stream, &version, &stamp, &kind, &payload, &correlation); err != nil {
			return nil, err
		}
		at, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			return nil, err
		}
		event, err := s.registry.DeserializeEvent(ctx, kind, payload)
		if err != nil {
			return nil, err
		}
		result = append(result, record{position, es.NewEnvelope(topic, org, stream, at, version, event, correlation)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func envelopes(records []record) []es.Envelope {
	result := make([]es.Envelope, 0, len(records))
	for _, r := range records {
		result = append(result, r.envelope)
	}
	return result
}

// GetEvents reads ascending stream versions, including fromVersion.
func (s *Store) GetEvents(ctx context.Context, topic, stream string, fromVersion int, limit *int) ([]es.Envelope, error) {
	if !valid(topic) || !valid(stream) || fromVersion < 0 || (limit != nil && *limit <= 0) {
		return nil, fmt.Errorf("invalid stream query")
	}
	q := "SELECT " + columns + " FROM es_events WHERE topic=? AND stream=? AND version>=? ORDER BY version"
	args := []any{topic, stream, fromVersion}
	if limit != nil {
		q += " LIMIT ?"
		args = append(args, *limit)
	}
	records, err := s.read(ctx, q, args...)
	return envelopes(records), err
}

// GetLatestEvent returns nil when no event exists for the stream.
func (s *Store) GetLatestEvent(ctx context.Context, topic, stream string) (*es.Envelope, error) {
	if !valid(topic) || !valid(stream) {
		return nil, fmt.Errorf("topic and stream are required")
	}
	records, err := s.read(ctx, "SELECT "+columns+" FROM es_events WHERE topic=? AND stream=? ORDER BY version DESC LIMIT 1", topic, stream)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	return &records[0].envelope, nil
}

type cursor struct {
	Store     string `json:"store"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Position  int64  `json:"position"`
	Format    int    `json:"format"`
}

func (s *Store) encode(topic string, partition int, position int64) string {
	data, _ := json.Marshal(cursor{s.id, topic, partition, position, 1})
	return base64.RawURLEncoding.EncodeToString(data)
}
func (s *Store) decode(value, topic string, partition int) (int64, error) {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, fmt.Errorf("invalid cursor encoding: %w", es.ErrCursorNotFound)
	}
	var c cursor
	if json.Unmarshal(data, &c) != nil || c.Format != 1 || c.Store != s.id || c.Topic != topic || c.Partition != partition || c.Position < 0 {
		return 0, fmt.Errorf("invalid cursor scope: %w", es.ErrCursorNotFound)
	}
	return c.Position, nil
}

func (s *Store) scopeFrom(ctx context.Context, source queryer, topic string, partition int) error {
	var count int
	if err := source.QueryRowContext(ctx, "SELECT partitions FROM es_topics WHERE topic=?", topic).Scan(&count); err != nil {
		return err
	}
	if partition < -1 || partition >= count {
		return fmt.Errorf("invalid partition")
	}
	return nil
}

// GetTailCursor captures the committed end of a topic partition without decoding
// history. Sequence ordering is independent of recording timestamps.
func (s *Store) GetTailCursor(ctx context.Context, topic string, partition int) (string, error) {
	return s.tailFrom(ctx, s.db, topic, partition)
}

func (s *Store) tailFrom(ctx context.Context, source queryer, topic string, partition int) (string, error) {
	if !valid(topic) || partition < 0 {
		return "", fmt.Errorf("topic and nonnegative partition required")
	}
	if err := s.scopeFrom(ctx, source, topic, partition); err != nil {
		return "", err
	}
	var position int64
	if err := source.QueryRowContext(ctx, "SELECT COALESCE(MAX(position),0) FROM es_events WHERE topic=? AND partition=?", topic, partition).Scan(&position); err != nil {
		return "", err
	}
	return s.encode(topic, partition, position), nil
}

// GetEventsByTopic pages one partition strictly after an opaque position.
func (s *Store) GetEventsByTopic(ctx context.Context, topic string, partition int, value string) (es.EventsPage, error) {
	if partition < 0 {
		return es.EventsPage{}, fmt.Errorf("partition must be nonnegative")
	}
	return s.GetEventsByOrgs(ctx, topic, partition, nil, value)
}

// GetEventsByOrgs pages a topic in committed sequence order. Empty orgs selects
// all organisations; partition -1 selects all partitions. This is not auth.
func (s *Store) GetEventsByOrgs(ctx context.Context, topic string, partition int, orgs []string, value string) (es.EventsPage, error) {
	return s.pageFrom(ctx, s.db, topic, partition, orgs, value)
}

func (s *Store) pageFrom(ctx context.Context, source queryer, topic string, partition int, orgs []string, value string) (es.EventsPage, error) {
	page := es.EventsPage{NextCursor: value}
	if !valid(topic) || partition < -1 {
		return page, fmt.Errorf("invalid topic or partition")
	}
	var position int64
	if value != "" {
		var err error
		position, err = s.decode(value, topic, partition)
		if err != nil {
			return page, err
		}
	}
	if err := s.scopeFrom(ctx, source, topic, partition); err != nil {
		return page, err
	}
	q := "SELECT " + columns + " FROM es_events WHERE topic=? AND position>?"
	args := []any{topic, position}
	if partition >= 0 {
		q += " AND partition=?"
		args = append(args, partition)
	}
	if len(orgs) > 0 {
		q += " AND org IN (" + strings.TrimSuffix(strings.Repeat("?,", len(orgs)), ",") + ")"
		for _, org := range orgs {
			args = append(args, org)
		}
	}
	q += " ORDER BY position LIMIT ?"
	args = append(args, s.pageSize)
	records, err := s.readFrom(ctx, source, q, args...)
	if err != nil {
		return page, err
	}
	page.Events = envelopes(records)
	if len(records) > 0 {
		page.NextCursor = s.encode(topic, partition, records[len(records)-1].position)
	}
	return page, nil
}

package spannerstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/traego/es"
)

type cursorData struct {
	Topic     string
	Partition int
	Timestamp time.Time
	Stream    string
	Version   int
}

type cursorWire struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Timestamp string `json:"timestamp"`
	Stream    string `json:"stream"`
	Version   int    `json:"version"`
}

func encodeCursor(cursor cursorData) string {
	// All wire fields are primitive strings/integers: JSON encoding cannot fail.
	// Database timestamps have already been validated by Spanner's row decoder.
	data, _ := json.Marshal(cursorWire{cursor.Topic, cursor.Partition, cursor.Timestamp.UTC().Format(time.RFC3339Nano), cursor.Stream, cursor.Version})
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeCursor(value, topic string, partition int) (cursorData, error) {
	var cursor cursorData
	var wire cursorWire
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return cursor, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return cursor, fmt.Errorf("invalid cursor data: %w", err)
	}
	stamp, err := time.Parse(time.RFC3339Nano, wire.Timestamp)
	if err != nil {
		return cursor, fmt.Errorf("invalid cursor timestamp: %w", err)
	}
	cursor = cursorData{wire.Topic, wire.Partition, stamp, wire.Stream, wire.Version}
	if cursor.Topic != topic || cursor.Partition != partition || cursor.Timestamp.IsZero() || !nonempty(cursor.Stream) || cursor.Version <= 0 {
		return cursor, fmt.Errorf("invalid cursor or wrong topic/partition")
	}
	return cursor, nil
}

func (s *Store) decodeEvent(ctx context.Context, row *spanner.Row) (es.Envelope, error) {
	var topic, org, stream, eventType, correlation string
	var timestamp time.Time
	var version int64
	var data []byte
	if err := row.Columns(&topic, &org, &stream, &timestamp, &version, &eventType, &data, &correlation); err != nil {
		return es.Envelope{}, fmt.Errorf("decode event row: %w", err)
	}
	event, err := s.registry.DeserializeEvent(ctx, eventType, data)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("decode event payload: %w", err)
	}
	return es.NewEnvelope(topic, org, stream, timestamp, int(version), event, correlation), nil
}

// GetEventsByTopic pages through one partition, preserving the cursor when idle.
func (s *Store) GetEventsByTopic(ctx context.Context, topic string, partition int, cursor string) (es.EventsPage, error) {
	if partition < 0 {
		return es.EventsPage{}, fmt.Errorf("partition must not be negative")
	}
	return s.GetEventsByOrgs(ctx, topic, partition, nil, cursor)
}

// GetEventsByOrgs reads tenant-filtered events. Empty orgIDs means all tenants;
// partition -1 means all partitions. This method is not an authorization check.
func (s *Store) GetEventsByOrgs(ctx context.Context, topic string, partition int, orgIDs []string, cursor string) (es.EventsPage, error) {
	result := es.EventsPage{NextCursor: cursor}
	if !nonempty(topic) || partition < -1 {
		return result, fmt.Errorf("topic and partition >= -1 are required")
	}
	stmt := spanner.Statement{SQL: "SELECT " + eventColumns + " FROM Events WHERE topic = @topic", Params: map[string]any{"topic": topic, "limit": int64(s.pageSize)}}
	if partition >= 0 {
		stmt.SQL += " AND partition_id = @partition"
		stmt.Params["partition"] = int64(partition)
	}
	if len(orgIDs) > 0 {
		stmt.SQL += " AND org_id IN UNNEST(@orgs)"
		stmt.Params["orgs"] = orgIDs
	}
	if cursor != "" {
		data, err := decodeCursor(cursor, topic, partition)
		if err != nil {
			return result, err
		}
		stmt.SQL += " AND (global_timestamp > @timestamp OR (global_timestamp = @timestamp AND stream_name > @stream) OR (global_timestamp = @timestamp AND stream_name = @stream AND version > @version))"
		stmt.Params["timestamp"] = data.Timestamp
		stmt.Params["stream"] = data.Stream
		stmt.Params["version"] = int64(data.Version)
	}
	stmt.SQL += " ORDER BY global_timestamp ASC, stream_name ASC, version ASC LIMIT @limit"
	rows, err := s.db.read(ctx, spanner.Statement{SQL: "SELECT partition_count FROM Topics WHERE topic_name = @topic", Params: map[string]any{"topic": topic}})
	if err != nil {
		return result, fmt.Errorf("read topic: %w", err)
	}
	if len(rows) == 0 {
		return result, fmt.Errorf("unknown topic %q", topic)
	}
	var count int64
	if err := rows[0].Column(0, &count); err != nil {
		return result, fmt.Errorf("decode partition count: %w", err)
	}
	if count <= 0 || uint64(count) > 1<<32-1 || int64(partition) >= count {
		return result, fmt.Errorf("invalid topic partition count or partition out of range")
	}
	events, err := s.readEvents(ctx, stmt)
	if err != nil {
		return result, err
	}
	result.Events = events
	if len(events) > 0 {
		last := events[len(events)-1]
		result.NextCursor = encodeCursor(cursorData{Topic: topic, Partition: partition, Timestamp: last.GetGlobalPosition(), Stream: last.GetStreamName(), Version: last.GetVersion()})
	}
	return result, nil
}

func validSubscription(topic, subscriber string, partition int) bool {
	return nonempty(topic) && nonempty(subscriber) && partition >= 0
}

// GetSubscriptionCursor returns es.ErrCursorNotFound before the first save.
func (s *Store) GetSubscriptionCursor(ctx context.Context, topic, subscriber string, partition int) (string, error) {
	if !validSubscription(topic, subscriber, partition) {
		return "", fmt.Errorf("topic, subscriber, and nonnegative partition are required")
	}
	rows, err := s.db.read(ctx, spanner.Statement{SQL: "SELECT cursor FROM Cursors WHERE topic = @topic AND subscriber_id = @subscriber AND partition_id = @partition", Params: map[string]any{"topic": topic, "subscriber": subscriber, "partition": int64(partition)}})
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", es.ErrCursorNotFound
	}
	var cursor string
	if err := rows[0].Column(0, &cursor); err != nil {
		return "", err
	}
	return cursor, nil
}

// SaveSubscriptionCursor saves a position, or resets it when cursor is empty.
// Consumers sharing a subscriber/partition must coordinate ownership externally.
func (s *Store) SaveSubscriptionCursor(ctx context.Context, topic, subscriber string, partition int, cursor string) error {
	if !validSubscription(topic, subscriber, partition) {
		return fmt.Errorf("topic, subscriber, and nonnegative partition are required")
	}
	if cursor != "" {
		if _, err := decodeCursor(cursor, topic, partition); err != nil {
			return err
		}
	}
	return s.db.apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("Cursors", []string{"topic", "subscriber_id", "partition_id", "cursor", "updated_at"}, []any{topic, subscriber, int64(partition), cursor, spanner.CommitTimestamp})})
}

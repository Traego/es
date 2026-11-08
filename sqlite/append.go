package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/traego/es"
)

type prepared struct {
	write     es.WriteEvents
	types     []string
	payloads  [][]byte
	partition int
	previous  int
}

func nilValue(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Interface, reflect.Chan:
		return r.IsNil()
	}
	return false
}

func prepare(ctx context.Context, writes []es.WriteEvents) ([]prepared, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([]prepared, 0, len(writes))
	seen := map[[2]string]bool{}
	for _, w := range writes {
		if !valid(w.Topic) || !valid(w.StreamName) || !valid(w.OrgId) || (w.ExpectedVersion != nil && *w.ExpectedVersion < 0) {
			return nil, fmt.Errorf("valid topic, stream, org and expected version are required")
		}
		key := [2]string{w.Topic, w.StreamName}
		if seen[key] {
			return nil, fmt.Errorf("duplicate topic/stream in batch")
		}
		seen[key] = true
		p := prepared{write: w}
		for _, event := range w.Events {
			if nilValue(event) || reflect.TypeOf(event).Kind() != reflect.Pointer || !valid(event.GetEventType()) || nilValue(event.GetEventCodec()) {
				return nil, fmt.Errorf("event pointer, type and codec are required")
			}
			if org, ok := event.(es.OrgEvent); ok && org.GetOrgId() != "" && org.GetOrgId() != w.OrgId {
				return nil, fmt.Errorf("event org does not match write org")
			}
			payload, err := event.GetEventCodec().Serialize(ctx, event)
			if err != nil {
				return nil, err
			}
			p.types = append(p.types, event.GetEventType())
			p.payloads = append(p.payloads, append([]byte{}, payload...))
		}
		result = append(result, p)
	}
	return result, nil
}

// SaveEvents commits a batch and its expected-version checks in one transaction.
func (s *Store) SaveEvents(ctx context.Context, writes []es.WriteEvents) error {
	prepared, err := prepare(ctx, writes)
	if err != nil {
		return err
	}
	if len(prepared) == 0 {
		return nil
	}
	return s.Transact(ctx, func(tx *Transaction) error { return tx.append(ctx, prepared) })
}

// SaveEvents appends inside the shared transaction. Failure prevents the outer
// commit. Success is provisional until Transact returns nil.
func (tx *Transaction) SaveEvents(ctx context.Context, writes []es.WriteEvents) error {
	if !tx.active {
		return ErrTransactionClosed
	}
	prepared, err := prepare(ctx, writes)
	if err == nil {
		err = tx.append(ctx, prepared)
	}
	return tx.remember(err)
}

func (tx *Transaction) append(ctx context.Context, batch []prepared) error {
	// Validate every stream before any append, inside the immediate transaction.
	for i := range batch {
		p := &batch[i]
		w := p.write
		var count int
		if err := tx.sql.QueryRowContext(ctx, "SELECT partitions FROM es_topics WHERE topic=?", w.Topic).Scan(&count); err != nil {
			return fmt.Errorf("read topic: %w", err)
		}
		key := w.PartitionKey
		if key == "" {
			key = w.StreamName
		}
		partition, err := es.CalculatePartitionId(key, count)
		if err != nil {
			return err
		}
		p.partition = partition
		var org string
		var existingPartition int
		err = tx.sql.QueryRowContext(ctx, "SELECT version,org,partition FROM es_streams WHERE topic=? AND stream=?", w.Topic, w.StreamName).Scan(&p.previous, &org, &existingPartition)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if w.ExpectedVersion != nil && *w.ExpectedVersion != p.previous {
			return &es.OptimisticConcurrencyErr{StreamName: w.StreamName, ExpectedVersion: *w.ExpectedVersion, ActualVersion: p.previous}
		}
		if err == nil && (org != w.OrgId || existingPartition != partition) {
			return fmt.Errorf("stream org and partition are immutable")
		}
	}
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	for _, p := range batch {
		if len(p.payloads) == 0 {
			continue
		}
		w := p.write
		if _, err := tx.sql.ExecContext(ctx, "INSERT INTO es_streams(topic,stream,org,partition,version) VALUES(?,?,?,?,?) ON CONFLICT(topic,stream) DO UPDATE SET version=excluded.version", w.Topic, w.StreamName, w.OrgId, p.partition, p.previous+len(p.payloads)); err != nil {
			return err
		}
		for i, payload := range p.payloads {
			if _, err := tx.sql.ExecContext(ctx, "INSERT INTO es_events(topic,stream,version,org,partition,recorded_at,event_type,payload,correlation) VALUES(?,?,?,?,?,?,?,?,?)", w.Topic, w.StreamName, p.previous+i+1, w.OrgId, p.partition, stamp, p.types[i], payload, w.CorrelationId); err != nil {
				return err
			}
		}
	}
	return nil
}

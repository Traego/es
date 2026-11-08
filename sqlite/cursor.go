package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/traego/es"
)

func subscription(topic, subscriber string, partition int) error {
	if !valid(topic) || !valid(subscriber) || partition < 0 {
		return fmt.Errorf("valid topic, subscriber and nonnegative partition required")
	}
	return nil
}

// GetSubscriptionCursor distinguishes absent rows from explicitly saved empty
// checkpoints, using the same ErrCursorNotFound sentinel as other adapters.
func (s *Store) GetSubscriptionCursor(ctx context.Context, topic, subscriber string, partition int) (string, error) {
	if err := subscription(topic, subscriber, partition); err != nil {
		return "", err
	}
	var value string
	err := s.db.QueryRowContext(ctx, "SELECT cursor FROM es_cursors WHERE topic=? AND subscriber=? AND partition=?", topic, subscriber, partition).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", es.ErrCursorNotFound
	}
	return value, err
}

// SaveSubscriptionCursor saves a checkpoint or explicitly resets it to beginning
// with "". Applications coordinate one owner per subscriber and partition.
func (s *Store) SaveSubscriptionCursor(ctx context.Context, topic, subscriber string, partition int, value string) error {
	return s.Transact(ctx, func(tx *Transaction) error {
		return tx.SaveSubscriptionCursor(ctx, topic, subscriber, partition, value)
	})
}

// SaveSubscriptionCursor saves a checkpoint in the same transaction as events or
// application projection rows. Success is provisional until Transact commits.
func (tx *Transaction) SaveSubscriptionCursor(ctx context.Context, topic, subscriber string, partition int, value string) error {
	if !tx.active {
		return ErrTransactionClosed
	}
	err := subscription(topic, subscriber, partition)
	if err == nil && value != "" {
		_, err = tx.store.decode(value, topic, partition)
	}
	if err != nil {
		return tx.remember(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO es_cursors(topic,subscriber,partition,cursor) VALUES(?,?,?,?) ON CONFLICT(topic,subscriber,partition) DO UPDATE SET cursor=excluded.cursor", topic, subscriber, partition, value)
	return err
}

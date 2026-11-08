package sqlitestore

import (
	"context"
	"errors"
	"fmt"

	"github.com/traego/es"
)

// MaterializerConfig identifies one projection consumer on one topic partition.
// Use a separate subscriber ID for each view/version. Callers schedule batches;
// this API does not start background workers or automatically retry callbacks.
type MaterializerConfig struct {
	// SubscriberID names this projection's checkpoint in the cursor table.
	SubscriberID string
	// Topic is the ensured event topic to consume.
	Topic string
	// Partition is one nonnegative topic partition.
	Partition int
	// StartPosition applies only if no checkpoint exists. Zero is StartBeginning.
	StartPosition es.StartPosition
}

// MaterializationResult reports committed progress, never provisional updates.
// Zero Processed means no events were available at this transaction's snapshot.
type MaterializationResult struct {
	// Processed counts envelopes passed to the handler in the committed batch.
	Processed int
	// Cursor is the committed checkpoint, including when an initial topic is empty.
	Cursor string
}

// ProjectionHandler applies one recorded event to a view using the supplied
// transaction. Return all query/scan/iteration errors. Do not use Store methods,
// perform external actions or manage this subscription's checkpoint here. The
// handler receives the actual envelope, not a newer aggregate snapshot.
type ProjectionHandler func(context.Context, *Transaction, es.Envelope) error

// MaterializeBatch atomically reads a checkpoint and one event page, updates the
// projection and advances its checkpoint in the same SQLite transaction. The page
// size is the Store's configured PageSize. Saved checkpoints override the initial
// start policy; an absent checkpoint is initialized even for an empty topic.
//
// All SQL reads use this transaction, including the checkpoint and event page.
// Failures and panics roll back all view/checkpoint writes; panics propagate.
// Handlers run once per attempted page and must keep effects in this database.
// The existing es.Materializer and StreamProcessor retain their separate behavior.
func (s *Store) MaterializeBatch(ctx context.Context, cfg MaterializerConfig, handler ProjectionHandler) (MaterializationResult, error) {
	if err := subscription(cfg.Topic, cfg.SubscriberID, cfg.Partition); err != nil {
		return MaterializationResult{}, err
	}
	if handler == nil || (cfg.StartPosition != es.StartBeginning && cfg.StartPosition != es.StartTail) {
		return MaterializationResult{}, fmt.Errorf("projection handler and valid start position are required")
	}
	var result MaterializationResult
	err := s.Transact(ctx, func(tx *Transaction) error {
		cursor, err := s.checkpointFrom(ctx, tx.sql, cfg.Topic, cfg.SubscriberID, cfg.Partition)
		if errors.Is(err, es.ErrCursorNotFound) {
			cursor = ""
			if cfg.StartPosition == es.StartTail {
				cursor, err = s.tailFrom(ctx, tx.sql, cfg.Topic, cfg.Partition)
				if err != nil {
					return err
				}
			}
			if err := tx.SaveSubscriptionCursor(ctx, cfg.Topic, cfg.SubscriberID, cfg.Partition, cursor); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		page, err := s.pageFrom(ctx, tx.sql, cfg.Topic, cfg.Partition, nil, cursor)
		if err != nil {
			return err
		}
		for _, event := range page.Events {
			if err := handler(ctx, tx, event); err != nil {
				return fmt.Errorf("materialize %s version %d: %w", event.GetStreamName(), event.GetVersion(), err)
			}
			if tx.failure != nil {
				return tx.failure
			}
		}
		if page.NextCursor != cursor {
			if err := tx.SaveSubscriptionCursor(ctx, cfg.Topic, cfg.SubscriberID, cfg.Partition, page.NextCursor); err != nil {
				return err
			}
		}
		result = MaterializationResult{Processed: len(page.Events), Cursor: page.NextCursor}
		return nil
	})
	if err != nil {
		return MaterializationResult{}, err
	}
	return result, nil
}

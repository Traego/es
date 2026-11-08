package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrTransactionClosed identifies use of a transaction outside its callback.
var ErrTransactionClosed = errors.New("SQLite transaction is closed")

// Transaction joins application SQL, event appends and checkpoints in one SQLite
// transaction. Use it only inside Transact's callback, from one goroutine. It is
// deliberately not an es.EventStore: an aggregate store or live engine must not
// acknowledge pending events or update its memory before the outer commit.
type Transaction struct {
	sql     *sql.Tx
	store   *Store
	active  bool
	failure error
}

func (tx *Transaction) remember(err error) error {
	if tx.failure == nil {
		tx.failure = err
	}
	return err
}

// Transact commits only after the callback succeeds. An event/checkpoint/SQL
// failure makes it rollback-only, even if the callback ignores that error. Panic
// also rolls back before propagating. Callbacks are not automatically retried;
// do not perform device/network side effects inside them. Use this transaction's
// methods, not Store methods, within the callback. Tables must be in this file.
func (s *Store) Transact(ctx context.Context, fn func(*Transaction) error) error {
	if fn == nil {
		return fmt.Errorf("transaction callback is required")
	}
	raw, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	tx := &Transaction{sql: raw, store: s, active: true}
	defer func() { tx.active = false; _ = raw.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if tx.failure != nil {
		return tx.failure
	}
	return raw.Commit()
}

// ExecContext executes application SQL on the same transaction as event writes.
// Application code must not issue transaction-control SQL or change es_ tables.
func (tx *Transaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if !tx.active {
		return nil, ErrTransactionClosed
	}
	result, err := tx.sql.ExecContext(ctx, query, args...)
	return result, tx.remember(err)
}

// QueryContext reads application rows inside this transaction. Close rows before
// returning from the callback and return any scan/iteration errors to Transact.
func (tx *Transaction) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if !tx.active {
		return nil, ErrTransactionClosed
	}
	rows, err := tx.sql.QueryContext(ctx, query, args...)
	return rows, tx.remember(err)
}

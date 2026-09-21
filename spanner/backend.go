package spannerstore

import (
	"context"

	"cloud.google.com/go/spanner"
)

// The small internal transport boundary lets unit tests exercise failures
// without replacing the storage semantics tested against the real emulator.
type reader interface {
	read(context.Context, spanner.Statement) ([]*spanner.Row, error)
}

type transaction interface {
	reader
	buffer([]*spanner.Mutation) error
}

type backend interface {
	reader
	transact(context.Context, func(transaction) error) error
	apply(context.Context, []*spanner.Mutation) error
}

type clientBackend struct{ client *spanner.Client }
type clientTransaction struct{ txn *spanner.ReadWriteTransaction }

func collect(iter *spanner.RowIterator) ([]*spanner.Row, error) {
	var rows []*spanner.Row
	err := iter.Do(func(row *spanner.Row) error {
		rows = append(rows, row)
		return nil
	})
	return rows, err
}

func (b clientBackend) read(ctx context.Context, stmt spanner.Statement) ([]*spanner.Row, error) {
	return collect(b.client.Single().Query(ctx, stmt))
}

func (b clientBackend) transact(ctx context.Context, f func(transaction) error) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		return f(clientTransaction{txn})
	})
	return err
}

func (b clientBackend) apply(ctx context.Context, mutations []*spanner.Mutation) error {
	_, err := b.client.Apply(ctx, mutations)
	return err
}

func (t clientTransaction) read(ctx context.Context, stmt spanner.Statement) ([]*spanner.Row, error) {
	return collect(t.txn.Query(ctx, stmt))
}

func (t clientTransaction) buffer(mutations []*spanner.Mutation) error {
	return t.txn.BufferWrite(mutations)
}

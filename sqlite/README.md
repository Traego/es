# SQLite event store

`github.com/traego/es/sqlite` is the SQLite adapter for Traego ES. It implements
`es.EventStore` without adding a SQLite dependency to the core library. It uses
pinned `modernc.org/sqlite`, a pure-Go driver, for Mac and 64-bit Raspberry Pi
builds without CGO. It requires Go 1.27.1 or newer and is not released yet;
work from this checkout with its sibling core module. Before publication, pin the
core dependency to a release containing the subscriber/tail interface changes.

## Open a local database

```go
store, err := sqlitestore.Open(ctx, "/var/lib/nexus/events.db", sqlitestore.Config{})
if err != nil {
    return err
}
defer store.Close()

// Register every historic event type before reading/replaying it.
if err := store.RegisterAggregate(ctx, &MyAggregate{}); err != nil {
    return err
}
aggregates := es.NewAggregateStore(store)
```

The parent directory must already exist, reside on a local filesystem, and have
appropriate private permissions. `Open` accepts an absolute filename rather than
an arbitrary connection URI and never falls back to an in-memory store. The
caller owns and closes the store after stopping consumers. Defaults are 50 events
per page, four pooled connections and a five-second busy timeout. Every connection
uses foreign keys, `synchronous=FULL`, and immediate write transactions. `Open`
enables WAL. A busy timeout shorter than one millisecond is rejected.

The adapter owns only names beginning with `es_`. It uses its own `es_schema`
version/identity table rather than claiming the application's `PRAGMA user_version`.
Schema creation is transactional; unsupported schema versions fail opening. This
first schema has no upgrade migration from another event-store layout.

## Transaction guarantees

- A stream is `(topic, stream)`; an event is uniquely `(topic, stream, version)`.
- Expected-version checks and all stream writes in a batch happen in the same
  immediate transaction. Failed batches leave no event or stream-metadata changes.
- The first event fixes the stream's organisation and partition. Topic partition
  counts are immutable. Zero expected version requires a new stream; nil appends
  without a version precondition.
- Events are append-only through the public adapter. A durable integer sequence
  orders pages independently of recording timestamps. Cursors are opaque, scoped
  to this database's persistent identity/topic/partition, and survive reopening.
- Checkpoints live in `es_cursors`, keyed by `(topic, subscriber, partition)`.
  Missing rows match `es.ErrCursorNotFound`; a saved empty string explicitly resets
  to the beginning. Malformed/wrong-scope cursors use the same sentinel. Other
  SQLite errors preserve their causes; optimistic conflicts use
  `es.OptimisticConcurrencyErr` and match `es.ErrOptimisticConcurrency`.
- A successful commit is acknowledged only after SQLite commits. WAL with FULL
  synchronization is the durability setting; hardware/filesystems must honor
  synchronization. Process-exit tests are not a claim of tested physical power loss.

## Application rows and events in one transaction

Application tables, event rows and consumer checkpoints can share one transaction
**when they live in the same database file**:

```go
err := store.Transact(ctx, func(tx *sqlitestore.Transaction) error {
    if _, err := tx.ExecContext(ctx,
        "UPDATE device_projection SET value = ? WHERE id = ?", value, id,
    ); err != nil {
        return err
    }
    if err := tx.SaveEvents(ctx, writes); err != nil {
        return err
    }
    return tx.SaveSubscriptionCursor(ctx, topic, subscriber, partition, next)
})
```

`Transact` owns commit/rollback. Its transaction supports `ExecContext`,
`QueryContext`, `SaveEvents` and `SaveSubscriptionCursor`. A failed event,
checkpoint or SQL operation makes the transaction rollback-only even if the
callback ignores that operation's error. Return scan/iteration errors from
application queries too. Close returned rows before leaving the callback. Panics
roll back and propagate. A transaction cannot be used after the callback ends.

Callbacks run once, with no automatic retry. After a concurrency conflict, reload
and reconsider the command. A commit error can have an uncertain outcome; do not
blindly replay a command. Coordinate one subscriber/partition owner externally.

Within the callback use only the transaction's methods for database work. Calling
Store methods instead starts independent work and can wait on the transaction's
own lock. Do not issue transaction-control SQL, change synchronization settings,
modify `es_` objects directly, or attach another database. Do not perform network
or device actions inside the callback; record an outbox/request and act after
commit instead. Files and external services do not participate in this transaction.

### The important in-memory boundary

`Transaction` deliberately does **not** implement `es.EventStore`. Passing an
uncommitted store to `es.NewAggregateStore` would let `SaveBatch` clear an
aggregate's pending events before the outer transaction commits. A live Nexus
engine could likewise publish an in-memory read model that the database later
rolls back. This adapter does not pretend that rolling back SQL rolls back Go
objects.

Normal `es.NewAggregateStore(store).Save/SaveBatch` is safe: the event-store call
commits before returning and pending events are cleared afterward. For a shared
application transaction, work with event write batches and staged application
state; publish changes/notifications only after `Transact` succeeds. Higher-level
engine participation needs an explicit application unit of work with post-commit
publication. That is separate from this adapter and does not require SQL in the
engine's ordinary storage interface.

An atomic projection/checkpoint update also requires an application-owned consume
loop using `Transact`. The existing StreamProcessor invokes handlers and saves
its checkpoint afterward; it is still at-least-once and is not silently made an
exactly-once transactional consumer by selecting this adapter.

## Backup and portability

Stop all writers and close their store handles before copying the database and
any remaining WAL files as one consistent set. A live copy of only the `.db` file
is not a supported backup. Online backup tooling is a separate future feature.
Test restore before depending on a backup. This adapter does not manage secrets
or encrypt the file.

Database cursors are not portable to another database/backend. A future export
must preserve events, metadata and versions and explicitly reset or translate
consumer checkpoints. Spanner's physical schema and transactions remain in its
own adapter.

## Verify

```sh
go vet ./...
go test -race -covermode=atomic -coverprofile=coverage.out ./...
../scripts/check-coverage.sh coverage.out
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...
```

Tests use actual temporary SQLite files, two independently opened stores racing
on the same stream, shared-table transactions, reopened checkpoints and child
processes that exit before/after commit. A connection wrapper injects database
failures while delegating ordinary operations to real SQLite. No physical Pi,
SD-card power interruption or Spanner instance is needed for these tests.

References: [SQLite transactions](https://www.sqlite.org/lang_transaction.html),
[WAL](https://www.sqlite.org/wal.html),
[synchronization](https://www.sqlite.org/pragma.html#pragma_synchronous),
[driver documentation](https://pkg.go.dev/modernc.org/sqlite@v1.62.0).

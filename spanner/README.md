# Spanner event store

`github.com/traego/es/spanner` is the optional Google Cloud Spanner adapter for
[`github.com/traego/es`](https://github.com/traego/es). Its Go package name is
`spannerstore`. The core module does not depend on Google Cloud libraries. This adapter
requires Go 1.25 or newer.

```go
import (
    "context"

    "cloud.google.com/go/spanner"
    "github.com/traego/es"
    spannerstore "github.com/traego/es/spanner"
)

func openStore(ctx context.Context, client *spanner.Client, root es.AggregateRoot) (*spannerstore.Store, error) {
    store, err := spannerstore.New(client, spannerstore.Config{PageSize: 100})
    if err != nil {
        return nil, err
    }
    if err := store.RegisterAggregate(ctx, root); err != nil {
        return nil, err
    }
    return store, nil
}
```

The caller owns and closes the Spanner client. A zero page size selects 50;
negative sizes are invalid. Register aggregate event prototypes before reading
historic events. Direct event-store writers can call `EnsureTopic` to create the
topic, but readers still need the aggregate's registered codecs.

## Database setup

Create an empty GoogleSQL database, then apply `SchemaDDL()` with your deployment
tool or call `CreateSchema(ctx, databaseAdminClient, databaseResourceName)` once.
The helper waits for DDL completion. It uses the fixed `Events`, `Topics`, and
`Cursors` tables defined in the embedded `schema.sql`. It returns an error on
existing tables and does not inspect or automatically upgrade old schemas.

The schema uses `(topic, stream_name, version)` as the event primary key.
Secondary indexes support partition and tenant polling. Deployment credentials
need schema permissions only when applying DDL; runtime clients need the reads
and writes used by the adapter.

## Storage contract

- A stream's identity is its topic and stream name. Batch writes across streams
  and topics commit in one read/write transaction. Expected versions are checked
  in that transaction, so conflicting writers cannot both succeed.
- Event history uses insert-only mutations. Version numbers begin at 1. A nil
  expected version appends unconditionally; zero requires an empty stream.
- `WriteEvents.OrgId` determines the envelope tenant. A nonempty tenant exposed
  by an `es.OrgEvent` must match it. Topic, stream name, and tenant are required.
- The first event establishes the stream's tenant and partition. Later appends
  cannot change either. A different partition key is allowed if it hashes to the
  same partition. Empty writes do not establish stream metadata.
- An empty partition key falls back to the stream name. Ensure topics before
  writing; partition counts must be positive, fit in 32 bits, and stay fixed.
  Repartitioning requires a separate migration protocol.
- Topic polling reads one valid partition. Organization polling accepts `-1`
  for all partitions; an empty organization list means all tenants. Filtering is
  not an authorization mechanism. Unknown topics and out-of-range partitions
  return errors. Stream reads return an empty result for an absent stream.
- Pages are ordered by commit timestamp, stream name, then version. Opaque
  cursors include their topic and partition and retain their position when no
  new events arrive. `GetEvents` includes `fromVersion` and requires a positive
  limit when supplied.
- Stored subscriber cursors are independent of application side effects.
  Processing is at least once: handlers should be idempotent. Coordinate one
  owner for each subscriber/partition; cursor saves are last-writer-wins.
  Saving an empty cursor explicitly resets that subscription.

## Migrating from `traego-svcs/shared/es`

This is a new schema and cursor format, not an in-place upgrade. The original
event table used `(stream_name, version)` as its primary key and could conflate
streams across topics. Do not run `CreateSchema` against that existing database
and expect an upgrade.

Before migration, stop writes and consumers, retain a backup, and inspect the
existing history for cross-topic version gaps, tenant mismatches, and streams
that moved partitions. Create the new schema in a separate database, then copy
validated event rows while preserving their payloads, metadata, commit times,
and per-stream order. Resolve any inconsistent histories explicitly; overwritten
historical events cannot be recovered from this adapter alone. Rebuild and
compare projections before cutover. Migrate subscriber checkpoints with a
dedicated conversion or deliberately replay into idempotent projections.

Old cursors and in-memory backend cursors are not interchangeable with this
adapter's cursors. Online partition-count changes are no longer accepted.
The library has no dependency on Traego's GCP configuration, migration manager,
logging setup, or other service modules.

## Tests and coverage

Unit tests exercise validation, malformed rows and payloads, transport failures,
concurrency conflicts, and cursor errors through a small internal transport
interface. The integration suite uses the actual Spanner emulator to test
transactions, rollback, competing writers, topic/tenant isolation, immutable
history, partition routing, reusable codec buffers, and resume after idle pages.
Each integration run creates a unique emulator instance and database and deletes
only its own resources on completion.

With an emulator running:

```sh
SPANNER_EMULATOR_HOST=127.0.0.1:9010 go test -race -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

The emulator suite skips when `SPANNER_EMULATOR_HOST` is unset. Full statement
coverage requires both unit and emulator tests; no production files are excluded.
The root repository's CI enforces the exact covered-statement count.

## Subscriber initialization

Checkpoints live in the `Cursors` table, keyed by topic, subscriber ID and
partition. Missing rows return `es.ErrCursorNotFound`; a saved empty value is an
explicit replay checkpoint. The processor chooses the initial policy using
`es.WithStartPosition(es.StartBeginning)` (default) or `es.StartTail`. A saved row
always takes precedence. Tail capture reads the last committed position with an
indexed metadata query; it does not replay or decode existing payloads. Initial
checkpoints are persisted before event processing, including for empty topics.
Malformed and wrong-scope cursor errors match `es.ErrCursorNotFound`.

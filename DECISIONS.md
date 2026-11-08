# Event-store decisions

## SQLite-first local persistence (2026-11-07)

Agreed with Pat: add SQLite as an adapter of this event-sourcing library alongside
Spanner. Preserve atomic batch appends, durable acknowledged commits, replay and
optimistic concurrency. Event uniqueness includes version: `(topic, stream_name,
version)`, with expected-version checks performed in the append transaction.
Use common error classifications across adapters. The SQLite adapter is the next
separate implementation slice; the library's engine-facing abstraction remains
`EventStore`.

## Subscription checkpoints and initial position (2026-11-07)

Agreed with Pat: durable subscription checkpoints live in a table keyed by
`(topic, subscriber_id, partition_id)`. Missing checkpoints return
`ErrCursorNotFound`. Subscribers configure beginning or tail startup only when no
checkpoint exists; the default remains beginning. A saved checkpoint, including
an explicitly saved empty cursor, wins over the configured default. Persist the
initial checkpoint before processing even if the topic is empty. With cursor
persistence disabled the policy applies independently to each run.

`GetTailCursor` is a backend operation so startup can capture an indexed end
position without reading all historic events. Its position is opaque and scoped
to a topic partition. It is captured per worker during initialization; this does
not promise one simultaneous boundary across partitions. Pages remain
at-least-once and are checkpointed only after successful handling. The
application must still coordinate one owner per subscriber/partition.

## SQLite shared transaction boundary (2026-11-07)

Implement SQLite in a separate `es/sqlite` module with a pinned pure-Go driver,
WAL, FULL synchronization and per-connection immediate transactions. Use namespaced
schema metadata and tables so application tables can share the database file.
Unique event keys are `(topic,stream,version)`; committed sequence positions order
pages independently of clock time. Checkpoints have a table, not a sidecar file.

`Store.Transact` joins low-level event writes, application SQL and checkpoint
updates on one transaction. Its transaction is rollback-only after a write/query
error even if the callback ignores that error. It exposes no commit operation and
is deliberately not an EventStore: AggregateStore clears pending events and a
live engine updates its read model after SaveEvents, which would be premature
inside an uncommitted outer transaction. Ordinary EventStore calls commit before
returning; higher-level engine participation requires staged memory and explicit
post-commit publication in the application. External device actions and other
files cannot be rolled back by SQLite. StreamProcessor remains at-least-once;
an atomic projection/checkpoint loop must deliberately use the shared transaction.

## Atomic materialized views and checkpoints (2026-11-07)

The next local slice is an event-by-event `sqlite.Store.MaterializeBatch` helper.
A batch loads its checkpoint, reads the event page, updates application view
rows and advances its checkpoint on one SQLite transaction. Success reports
committed progress only. Errors and process crashes cannot leave a view change
without its corresponding checkpoint. Stored checkpoints take precedence over
beginning/tail initialization; empty-topic initialization is durable too.

This is a bounded single-partition operation driven by the application, not a
second background worker framework. It deliberately consumes actual envelopes
instead of calling the generic materializer's latest-aggregate loader. The
existing Materializer and StreamProcessor keep their semantics. View handlers
stay inside the supplied transaction; external effects remain outside the
atomicity guarantee. SQLite-specific view SQL stays in application persistence
adapters, and the ordinary EventStore interface remains unchanged.

A reference counter view proves page-boundary state, atomic failure/retry,
independent subscribers/partitions, competing workers, restart, a quiescent
rebuild and process exit before/after commit. Full Nexus read-model persistence
and online view-version swaps are subsequent slices.

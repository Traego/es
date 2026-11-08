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

# Traego ES

[![CI](https://github.com/Traego/es/actions/workflows/ci.yml/badge.svg)](https://github.com/Traego/es/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/traego/es.svg)](https://pkg.go.dev/github.com/traego/es)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A Go event-sourcing library extracted from Traego's application infrastructure.
It separates domain aggregates, event persistence, partition consumers, and
read-model materialization. The core has no cloud SDK dependency; Cloud Spanner
support lives in a separate module.

## Install

```sh
go get github.com/traego/es@v0.1.0
# Optional persistent backend:
go get github.com/traego/es/spanner@v0.1.0
```

The core requires Go 1.24 or newer; the Spanner adapter requires Go 1.25 or
newer. Use a current supported Go release for production builds. This is a
pre-1.0 release; public APIs may evolve.

## What is included

- Aggregates with pending events, contiguous historical replay, and version checks.
- Atomic aggregate batch saves with optimistic concurrency, including new streams.
- A concurrency-safe in-memory event store that serializes events and isolates reads.
- An optional Spanner adapter with an embedded fresh-database schema.
- Event type registration, pluggable codecs, and a JSON codec.
- Partition consumers with cancellation, bounded retries, persistent cursors, panic recovery, and restart support.
- Current-state materializers for building application read models.
- Explicit organization metadata and correlation IDs.

## A complete example

The executable [counter example](example_test.go) defines an event and aggregate,
applies a command, commits its events, and restores state from history:

```sh
go test -run '^Example$' -v .
```

Once your aggregate implements `AggregateRoot`, setup is small:

```go
ctx := context.Background()
events := es.NewInMemoryEventStore()
if err := events.RegisterAggregate(ctx, &Counter{}); err != nil {
    return err
}
aggregates := es.NewAggregateStore(events)

// HandleCommand validates the command and calls es.ApplyEvent for each event.
if _, err := counter.HandleCommand(ctx, command); err != nil {
    return err
}
if err := aggregates.Save(ctx, counter); err != nil {
    // errors.Is(err, es.ErrOptimisticConcurrency) identifies stale state.
    return err
}
```

Embed `es.AggregateBase` in your domain aggregate. Keep `HandleEvent` deterministic
and free of external side effects so replay reconstructs the same state. Use
stable, versioned event names such as `counter.incremented.v1` and register their
codecs before reading durable history. The full example provides every required
method; the snippet above illustrates application integration.

## Persistence and concurrency contract

A stream is identified by `(topic, stream name)`. Aggregate stream names encode
aggregate type, organization, and ID using escaped components. Topic partition
counts are fixed once ensured. Ensure or register a topic before writing events.

`Save` and `SaveBatch` compare the previous committed aggregate version with
storage. Stale writes fail with `ErrOptimisticConcurrency`; pending events remain
on the aggregate. Reload a fresh aggregate and rerun the command after a conflict.
Do not blindly retry events derived from stale state. Aggregate objects are
single-owner values; stores are safe for concurrent use.

Direct `EventStore.SaveEvents` callers can set `ExpectedVersion`: zero requires a
new stream, a positive value checks the current stream, and nil appends without a
version precondition. A batch is all-or-nothing. The first event fixes a stream’s organization and
partition; later writes cannot change them. Its write envelope owns tenant
metadata; a nonempty event-level organization must agree with it.

Use `es.WithCorrelationID(ctx, id)` to associate related writes with a trace. If
absent, aggregate saves generate an ID shared by that batch.

## Consumers and materializers

Consumers process partitions concurrently and events within each partition in
order. A cursor is committed only after an entire page succeeds. Delivery is
**at least once**: a crash after a side effect but before checkpointing can replay
that side effect. Make handlers idempotent. Coordinate partition assignment
outside this library; it does not provide distributed leases or fencing.

Prefer `NewStreamProcessorWithContext` and `NewMaterializerWithContext` when
handlers perform I/O. Cancellation flows to stores and handlers. `Stop` waits
for handlers to return; do not call it from inside a handler. `Start` is
idempotent during an active run, and a completed processor can be restarted.
Drain its error channel to observe terminal failures.

Materializers load the **latest full aggregate state** for each notification.
They ignore canonical streams belonging to other aggregate types on the same
topic. They may see a version newer than the triggering event. Use them for current-state
views, not historical side effects. Snapshot storage is an extension interface;
automatic snapshotting, event upcasting, and incremental projection replay are
not implemented in this release. Retain compatible codecs for old events.

Cursors are opaque and backend-specific. They are not authorization tokens;
applications must authorize topic/organization filters independently. Durable
cursor writes are separate from application side effects.

## Migrating from traego-svcs

The original import path was `github.com/traego/traego-svcs/shared/es`.

- Replace it with `github.com/traego/es`; use the separate Spanner adapter module.
- Use `WithCorrelationID` instead of the monorepo tracing context.
- Retry configuration now belongs to `es`, without the generic utility module.
- Migration configuration is removed from the `EventStore` interface.
- Aggregate stream names now use escaped slash-separated components, so old
  aggregate IDs are not automatically readable under the new naming convention.
- The Spanner primary key now includes topic. Its schema is for a **fresh
  database**. Moving existing data requires an explicit migration for stream
  names, schema, and cursors. See [the adapter README](spanner/README.md).
- In-memory topics must be ensured first. Partition counts cannot be changed.
- Replay rejects gaps and duplicate versions, and supports contiguous suffixes.

This repository does not modify existing Traego services or databases.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for commands. CI tests the core and adapter,
runs the race detector and vet, and checks exact **100% statement coverage** from
unfiltered Go coverage profiles. Adapter tests include a real Spanner emulator.
Coverage is an execution measure; the tests also assert storage and recovery
behavior, including regressions for the defects found during extraction.

## License

Apache-2.0. Copyright 2025–2026 Pat White. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

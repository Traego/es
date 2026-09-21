// Package es provides aggregates, event persistence contracts, an in-memory
// store, partitioned stream consumers, and current-state materializers.
//
// Embed AggregateBase in domain aggregates. HandleCommand validates business
// rules and calls ApplyEvent to evolve state and record pending events. Save
// and SaveBatch commit those events with optimistic concurrency. Load replays
// history into a fresh aggregate. Aggregate instances are not concurrency-safe.
//
// Stream processors deliver at least once and checkpoint only successful pages.
// Handlers must be idempotent. Use NewStreamProcessorWithContext for cancellation.
// Persistent Spanner storage is provided by the separate module
// github.com/traego/es/spanner; the core has no cloud SDK dependency.
package es

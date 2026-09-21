package es

import (
	"context"
	"errors"
)

// ApplyEvent tracks a new event in the aggregate and applies it to update the aggregate state.
// This function:
// 1. Applies the event to the aggregate root to make sure we're in a stable state
// 2. Adds the event to the list of uncommitted events
// 3. Advances the working version; persistence checks the prior committed version
func ApplyEvent(ctx context.Context, aggregateRoot AggregateRoot, event Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNil(aggregateRoot) || isNil(event) {
		return errors.New("aggregate and event are required")
	}
	base := aggregateRoot.getBase()

	// Apply the event to update the aggregate state
	if err := aggregateRoot.HandleEvent(ctx, event, nil); err != nil {
		return err
	}

	// Add to uncommitted events for later saving
	base.uncommittedEvents = append(base.uncommittedEvents, event)

	base.Version++

	return nil
}

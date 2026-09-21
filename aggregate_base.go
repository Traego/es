package es

import (
	"context"
	"fmt"
	"sort"
)

// AggregateBase is a common base implementation for aggregates
type AggregateBase struct {
	Id                string
	OrgId             string
	Version           int
	uncommittedEvents []Event
}

// GetId returns the ID of the aggregate
func (a *AggregateBase) GetId() string {
	return a.Id
}

func (a *AggregateBase) GetOrgId() string {
	return a.OrgId
}

// GetVersion returns the version of the aggregate
func (a *AggregateBase) GetVersion() int {
	return a.Version
}

// GetUncommittedEvents returns all uncommitted events
func (a *AggregateBase) GetUncommittedEvents() []Event {
	return append([]Event(nil), a.uncommittedEvents...)
}

// ClearUncommittedEvents clears all uncommitted events
func (a *AggregateBase) ClearUncommittedEvents() {
	a.uncommittedEvents = nil
}

func (a *AggregateBase) GetStreamName(root AggregateRoot) (string, error) {
	return GetStreamNameFromFullAgg(root)
}

// LoadHistoricEvents replays contiguous versions after the current version.
// It accepts unsorted input without mutating the caller's slice. On a handler
// error the instance may be partially updated and must be discarded.
func (a *AggregateBase) LoadHistoricEvents(ctx context.Context, events []Envelope, root AggregateRoot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNil(root) {
		return fmt.Errorf("aggregate is required")
	}
	ordered := append([]Envelope(nil), events...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].GetVersion() < ordered[j].GetVersion() })
	for i, env := range ordered {
		if isNil(env.GetEvent()) {
			return fmt.Errorf("historic event is required")
		}
		if env.GetVersion() != a.Version+i+1 {
			return NewEventHandlingError(env, fmt.Errorf("event version %d is not the next contiguous version %d", env.GetVersion(), a.Version+i+1))
		}
	}
	for _, env := range ordered {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := root.HandleEvent(ctx, env.GetEvent(), &env); err != nil {
			return NewEventHandlingError(env, err)
		}
		a.Version = env.GetVersion()
	}
	return ctx.Err()
}

// RecordEvent records that an event was applied during replay
func (a *AggregateBase) RecordEvent(event Event) {
	// No longer tracking applied events
}

// getBase returns the aggregate base instance
func (a *AggregateBase) getBase() *AggregateBase {
	return a
}

var _ AggregateBaseLifecycle = (*AggregateBase)(nil)

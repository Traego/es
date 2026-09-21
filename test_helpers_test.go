package es

import (
	"context"
	"testing"
)

func testMemoryStore(t *testing.T) *InMemoryEventStore {
	t.Helper()
	store := NewInMemoryEventStore()
	if err := store.RegisterAggregate(context.Background(), NewTestAggregate()); err != nil {
		t.Fatal(err)
	}
	return store
}

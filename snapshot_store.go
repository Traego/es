package es

import "context"

// SnapshotStore defines operations for storing and retrieving aggregate snapshots
type SnapshotStore interface {
	SaveSnapshot(ctx context.Context, aggregateId string, version int, data []byte) error
	GetLatestSnapshot(ctx context.Context, aggregateId string) (data []byte, version int, found bool, err error)
}

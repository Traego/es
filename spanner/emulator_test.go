package spannerstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/traego/es"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Every run uses a new instance/database on the explicitly selected emulator;
// cleanup deletes only those resources. No production credentials are used.
func emulatorStore(t *testing.T) (*Store, *spanner.Client, context.Context) {
	t.Helper()
	host := os.Getenv("SPANNER_EMULATOR_HOST")
	if host == "" {
		t.Skip("set SPANNER_EMULATOR_HOST to run real Spanner emulator tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	opts := []option.ClientOption{option.WithEndpoint(host), option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
	ia, err := instance.NewInstanceAdminClient(ctx, opts...)
	requireOK(t, err)
	t.Cleanup(func() { requireOK(t, ia.Close()) })
	project := "projects/es-tests"
	id := fmt.Sprintf("es-%d", time.Now().UnixNano())
	instanceName := project + "/instances/" + id
	op, err := ia.CreateInstance(ctx, &instancepb.CreateInstanceRequest{Parent: project, InstanceId: id, Instance: &instancepb.Instance{Config: project + "/instanceConfigs/emulator-config", DisplayName: "ephemeral es tests", NodeCount: 1}})
	requireOK(t, err)
	_, err = op.Wait(ctx)
	requireOK(t, err)
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		requireOK(t, ia.DeleteInstance(cleanupCtx, &instancepb.DeleteInstanceRequest{Name: instanceName}))
	})
	da, err := database.NewDatabaseAdminClient(ctx, opts...)
	requireOK(t, err)
	t.Cleanup(func() { requireOK(t, da.Close()) })
	dbOp, err := da.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{Parent: instanceName, CreateStatement: "CREATE DATABASE library_test"})
	requireOK(t, err)
	db, err := dbOp.Wait(ctx)
	requireOK(t, err)
	requireOK(t, CreateSchema(ctx, da, db.Name))
	// Existing schemas are explicitly rejected, and request errors propagate.
	requireError(t, CreateSchema(ctx, da, db.Name))
	requireError(t, CreateSchema(ctx, da, ""))
	stopped, stop := context.WithCancel(ctx)
	stop()
	requireError(t, CreateSchema(stopped, da, db.Name))
	client, err := spanner.NewClientWithConfig(ctx, db.Name, spanner.ClientConfig{DisableNativeMetrics: true}, opts...)
	requireOK(t, err)
	t.Cleanup(client.Close)
	s, err := New(client, Config{PageSize: 2})
	requireOK(t, err)
	requireOK(t, s.RegisterAggregate(ctx, &testAggregate{topic: "registered", count: 1, prototypes: []es.Event{&testEvent{}}}))
	return s, client, ctx
}

func batchWrite(topic, stream, org string, count int) es.WriteEvents {
	w := es.WriteEvents{Topic: topic, StreamName: stream, OrgId: org, CorrelationId: "request-123"}
	for i := 0; i < count; i++ {
		w.Events = append(w.Events, &testEvent{ID: stream, Name: fmt.Sprintf("event-%d", i+1)})
	}
	return w
}

type reusableCodec struct{ scratch []byte }

func (c *reusableCodec) Serialize(ctx context.Context, event es.Event) ([]byte, error) {
	data, err := (es.JsonEventCodec{}).Serialize(ctx, event)
	c.scratch = append(c.scratch[:0], data...)
	return c.scratch, err
}

func (*reusableCodec) Deserialize(ctx context.Context, data []byte, event es.Event) error {
	return (es.JsonEventCodec{}).Deserialize(ctx, data, event)
}

func TestEmulatorPersistence(t *testing.T) {
	s, client, ctx := emulatorStore(t)

	t.Run("topic namespace and tenant envelope", func(t *testing.T) {
		for _, topic := range []string{"alpha", "beta"} {
			requireOK(t, s.EnsureTopic(ctx, topic, 1))
		}
		a, b := batchWrite("alpha", "same-stream", "tenant-a", 1), batchWrite("beta", "same-stream", "tenant-b", 1)
		a.ExpectedVersion = intPtr(0)
		b.ExpectedVersion = intPtr(0)
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{a, b}))
		for _, want := range []es.WriteEvents{a, b} {
			events, err := s.GetEvents(ctx, want.Topic, want.StreamName, 0, nil)
			requireOK(t, err)
			if len(events) != 1 || events[0].GetVersion() != 1 || events[0].GetOrgId() != want.OrgId || events[0].GetCorrelationId() != "request-123" {
				t.Fatalf("wrong namespace/metadata: %#v", events)
			}
			// Payload has no org metadata; the write envelope is authoritative.
			if events[0].GetEvent().(*testEvent).Org != "" {
				t.Fatal("payload unexpectedly changed")
			}
		}
		page, err := s.GetEventsByOrgs(ctx, "alpha", 0, []string{"tenant-a"}, "")
		requireOK(t, err)
		if len(page.Events) != 1 {
			t.Fatal("tenant event lost")
		}
		page, err = s.GetEventsByOrgs(ctx, "alpha", 0, []string{"tenant-b"}, "")
		requireOK(t, err)
		if len(page.Events) != 0 {
			t.Fatal("cross-tenant event")
		}
	})

	t.Run("atomic rollback on stale stream", func(t *testing.T) {
		requireOK(t, s.EnsureTopic(ctx, "atomic", 1))
		existing := batchWrite("atomic", "existing", "org", 1)
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{existing}))
		fresh := batchWrite("atomic", "fresh", "org", 1)
		fresh.ExpectedVersion = intPtr(0)
		existing.ExpectedVersion = intPtr(0)
		err := s.SaveEvents(ctx, []es.WriteEvents{fresh, existing})
		if !errors.Is(err, es.ErrOptimisticConcurrency) {
			t.Fatalf("expected conflict, got %v", err)
		}
		events, err := s.GetEvents(ctx, "atomic", "fresh", 0, nil)
		requireOK(t, err)
		if len(events) != 0 {
			t.Fatal("partial batch committed")
		}
		latest, err := s.GetLatestEvent(ctx, "atomic", "existing")
		requireOK(t, err)
		if latest.GetVersion() != 1 {
			t.Fatal("history overwritten")
		}
	})

	t.Run("cursor tie ordering and idle resume", func(t *testing.T) {
		requireOK(t, s.EnsureTopic(ctx, "paging", 1))
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{batchWrite("paging", "z-stream", "org", 2), batchWrite("paging", "a-stream", "org", 2)}))
		first, err := s.GetEventsByTopic(ctx, "paging", 0, "")
		requireOK(t, err)
		second, err := s.GetEventsByTopic(ctx, "paging", 0, first.NextCursor)
		requireOK(t, err)
		if len(first.Events) != 2 || len(second.Events) != 2 {
			t.Fatal("page size")
		}
		if first.Events[0].GetStreamName() != "a-stream" || first.Events[0].GetVersion() != 1 || first.Events[1].GetVersion() != 2 || second.Events[0].GetStreamName() != "z-stream" || second.Events[1].GetVersion() != 2 {
			t.Fatal("incorrect tie order")
		}
		if !first.Events[0].GetGlobalPosition().Equal(second.Events[1].GetGlobalPosition()) {
			t.Fatal("one transaction must have one commit timestamp")
		}
		idle, err := s.GetEventsByOrgs(ctx, "paging", 0, []string{"org"}, second.NextCursor)
		requireOK(t, err)
		if len(idle.Events) != 0 || idle.NextCursor != second.NextCursor {
			t.Fatal("idle cursor lost")
		}
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{batchWrite("paging", "a-stream", "org", 1)}))
		resumed, err := s.GetEventsByTopic(ctx, "paging", 0, idle.NextCursor)
		requireOK(t, err)
		if len(resumed.Events) != 1 || resumed.Events[0].GetVersion() != 3 {
			t.Fatal("new commit skipped or history replayed")
		}
		from, err := s.GetEvents(ctx, "paging", "a-stream", 2, intPtr(1))
		requireOK(t, err)
		if len(from) != 1 || from[0].GetVersion() != 2 {
			t.Fatal("fromVersion/limit")
		}
		latest, err := s.GetLatestEvent(ctx, "paging", "a-stream")
		requireOK(t, err)
		if latest.GetVersion() != 3 {
			t.Fatal("latest version")
		}
		missing, err := s.GetLatestEvent(ctx, "paging", "missing")
		requireOK(t, err)
		if missing != nil {
			t.Fatal("missing stream")
		}
		_, err = s.GetSubscriptionCursor(ctx, "paging", "subscriber", 0)
		if !errors.Is(err, es.ErrCursorNotFound) {
			t.Fatalf("missing cursor: %v", err)
		}
		requireOK(t, s.SaveSubscriptionCursor(ctx, "paging", "subscriber", 0, resumed.NextCursor))
		cursor, err := s.GetSubscriptionCursor(ctx, "paging", "subscriber", 0)
		requireOK(t, err)
		if cursor != resumed.NextCursor {
			t.Fatal("cursor roundtrip")
		}
		requireOK(t, s.SaveSubscriptionCursor(ctx, "paging", "subscriber", 0, ""))
		cursor, err = s.GetSubscriptionCursor(ctx, "paging", "subscriber", 0)
		requireOK(t, err)
		if cursor != "" {
			t.Fatal("cursor reset")
		}
	})

	t.Run("partition fallback and immutable topology", func(t *testing.T) {
		requireOK(t, s.EnsureTopic(ctx, "partitioned", 3))
		requireOK(t, s.EnsureTopic(ctx, "partitioned", 3))
		requireError(t, s.EnsureTopic(ctx, "partitioned", 4))
		stream := "stream-with-partition-key"
		partition, err := es.CalculatePartitionId(stream, 3)
		requireOK(t, err)
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{batchWrite("partitioned", stream, "org", 1)}))
		page, err := s.GetEventsByTopic(ctx, "partitioned", partition, "")
		requireOK(t, err)
		if len(page.Events) != 1 {
			t.Fatal("fallback routed incorrectly")
		}
		all, err := s.GetEventsByOrgs(ctx, "partitioned", -1, nil, "")
		requireOK(t, err)
		if len(all.Events) != 1 {
			t.Fatal("all partitions")
		}
	})

	t.Run("optimistic competing writers", func(t *testing.T) {
		requireOK(t, s.EnsureTopic(ctx, "racing", 1))
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{batchWrite("racing", "stream", "org", 1)}))
		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				write := batchWrite("racing", "stream", "org", 1)
				write.ExpectedVersion = intPtr(1)
				results <- s.SaveEvents(ctx, []es.WriteEvents{write})
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		success, conflict := 0, 0
		for err := range results {
			if err == nil {
				success++
			} else if errors.Is(err, es.ErrOptimisticConcurrency) {
				conflict++
			} else {
				t.Fatal(err)
			}
		}
		if success != 1 || conflict != 1 {
			t.Fatalf("success=%d conflict=%d", success, conflict)
		}
		events, err := s.GetEvents(ctx, "racing", "stream", 0, nil)
		requireOK(t, err)
		if len(events) != 2 || events[0].GetVersion() != 1 || events[1].GetVersion() != 2 {
			t.Fatal("race corrupted history")
		}
	})

	t.Run("stream tenant and partition stay fixed", func(t *testing.T) {
		requireOK(t, s.EnsureTopic(ctx, "metadata", 3))
		first := batchWrite("metadata", "stream", "org-a", 1)
		first.PartitionKey = "initial-key"
		originalPartition, err := es.CalculatePartitionId(first.PartitionKey, 3)
		requireOK(t, err)
		sameKey, differentKey := "", ""
		for i := 0; sameKey == "" || differentKey == ""; i++ {
			key := fmt.Sprintf("candidate-%d", i)
			partition, err := es.CalculatePartitionId(key, 3)
			requireOK(t, err)
			if partition == originalPartition {
				sameKey = key
			} else {
				differentKey = key
			}
		}
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{first}))
		changed := first
		changed.OrgId = "org-b"
		fresh := batchWrite("metadata", "fresh", "org-a", 1)
		requireError(t, s.SaveEvents(ctx, []es.WriteEvents{fresh, changed}))
		missing, err := s.GetLatestEvent(ctx, "metadata", "fresh")
		requireOK(t, err)
		if missing != nil {
			t.Fatal("metadata error partially committed batch")
		}
		changed.ExpectedVersion = intPtr(0)
		if err := s.SaveEvents(ctx, []es.WriteEvents{changed}); !errors.Is(err, es.ErrOptimisticConcurrency) {
			t.Fatalf("concurrency must take precedence: %v", err)
		}
		changed = first
		changed.PartitionKey = differentKey
		requireError(t, s.SaveEvents(ctx, []es.WriteEvents{changed}))
		changed.PartitionKey = sameKey
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{changed}))
		latest, err := s.GetLatestEvent(ctx, "metadata", "stream")
		requireOK(t, err)
		if latest.GetVersion() != 2 || latest.GetOrgId() != "org-a" {
			t.Fatal("metadata changed")
		}
		empty := batchWrite("metadata", "empty", "first-org", 0)
		empty.PartitionKey = differentKey
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{empty}))
		created := batchWrite("metadata", "empty", "second-org", 1)
		created.PartitionKey = sameKey
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{created}))
		latest, err = s.GetLatestEvent(ctx, "metadata", "empty")
		requireOK(t, err)
		if latest.GetOrgId() != "second-org" {
			t.Fatal("empty write established tenant")
		}
	})

	t.Run("codec scratch buffers cannot corrupt a batch", func(t *testing.T) {
		requireOK(t, s.EnsureTopic(ctx, "codec-buffers", 1))
		codec := &reusableCodec{}
		write := batchWrite("codec-buffers", "stream", "org", 0)
		write.Events = []es.Event{&testEvent{ID: "stream", Name: "first", Codec: codec}, &testEvent{ID: "stream", Name: "later", Codec: codec}}
		requireOK(t, s.SaveEvents(ctx, []es.WriteEvents{write}))
		for i := range codec.scratch {
			codec.scratch[i] = 'x'
		}
		events, err := s.GetEvents(ctx, "codec-buffers", "stream", 0, nil)
		requireOK(t, err)
		if len(events) != 2 || events[0].GetEvent().(*testEvent).Name != "first" || events[1].GetEvent().(*testEvent).Name != "later" {
			t.Fatal("codec buffer changed history")
		}
	})

	t.Run("paging validates ensured topic and range", func(t *testing.T) {
		_, err := s.GetEventsByTopic(ctx, "not-ensured", 0, "")
		requireError(t, err)
		_, err = s.GetEventsByTopic(ctx, "registered", 1, "")
		requireError(t, err)
	})

	t.Run("insert-only history", func(t *testing.T) {
		// A duplicate primary key submitted directly to Spanner must fail; the
		// adapter uses the same insert mutation operation for every event.
		_, err := client.Apply(ctx, []*spanner.Mutation{spanner.Insert("Events", []string{"topic", "stream_name", "version", "global_timestamp", "partition_id", "org_id", "correlation_id", "event_type", "event_data"}, []any{"alpha", "same-stream", int64(1), spanner.CommitTimestamp, int64(0), "tenant-a", "", "test.created", []byte(`{}`)})})
		requireError(t, err)
		events, err := s.GetEvents(ctx, "alpha", "same-stream", 0, nil)
		requireOK(t, err)
		if len(events) != 1 || events[0].GetCorrelationId() != "request-123" {
			t.Fatal("immutable history changed")
		}
	})
}

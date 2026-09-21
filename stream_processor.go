package es

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// StreamProcessor processes each partition in order, with partitions running
// concurrently. Handlers must be concurrency-safe and idempotent: failed pages
// are replayed from the last persisted cursor (at-least-once delivery).
type StreamProcessor interface {
	// Start starts processing, or returns the current run's error channel when
	// already running or stopping. Configuration errors are returned on a
	// closed, buffered channel. A completed run can be restarted.
	Start() <-chan error
	// Stop cancels the active run and waits for all workers and error-channel
	// closure. It is safe to call concurrently or before Start. A handler must
	// return before Stop can complete; do not call Stop from a handler.
	Stop()
}

type processorRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	errors chan error
}

type streamProcessor struct {
	subscriberId            string
	topic                   string
	partitions              []int
	handler                 func(context.Context, Envelope) error
	pollingTime             time.Duration
	systemRetryConfig       RetryConfig
	processEventRetryConfig RetryConfig
	eventStore              EventStore
	persistCursor           bool
	parent                  context.Context
	mu                      sync.Mutex
	run                     *processorRun
}

// StreamProcessorOption configures a stream processor before it starts.
type StreamProcessorOption func(*streamProcessor)

// WithNewMessagePollingTime sets the positive polling interval.
func WithNewMessagePollingTime(duration time.Duration) StreamProcessorOption {
	return func(sp *streamProcessor) { sp.pollingTime = duration }
}

// WithSystemRetryConfig configures retries for event reads and cursor storage.
func WithSystemRetryConfig(config RetryConfig) StreamProcessorOption {
	return func(sp *streamProcessor) { sp.systemRetryConfig = config }
}

// WithProcessEventRetryConfig configures retries of unsuccessful handlers.
func WithProcessEventRetryConfig(config RetryConfig) StreamProcessorOption {
	return func(sp *streamProcessor) { sp.processEventRetryConfig = config }
}

// NewStreamProcessor constructs a processor with the original handler API.
// Use NewStreamProcessorWithContext when a handler needs shutdown cancellation.
func NewStreamProcessor(eventStore EventStore, subscriberId, topic string, partitions []int, persistCursor bool, handler func(Envelope) error, options ...StreamProcessorOption) StreamProcessor {
	var contextualHandler func(context.Context, Envelope) error
	if handler != nil {
		contextualHandler = func(_ context.Context, event Envelope) error { return handler(event) }
	}
	return NewStreamProcessorWithContext(context.Background(), eventStore, subscriberId, topic, partitions, persistCursor, contextualHandler, options...)
}

// NewStreamProcessorWithContext supplies a cancelable per-run context to store
// operations and the handler. Canceling parent stops the active run silently;
// restarting with an already canceled parent immediately completes. Each page
// is checkpointed only after every handler succeeds. A panic is a handler error.
// Options and input partitions are copied at construction; configuration must
// be valid when Start is called (including a nonempty, unique partition list).
func NewStreamProcessorWithContext(parent context.Context, eventStore EventStore, subscriberId, topic string, partitions []int, persistCursor bool, handler func(context.Context, Envelope) error, options ...StreamProcessorOption) StreamProcessor {
	sp := &streamProcessor{
		parent: parent, eventStore: eventStore, subscriberId: subscriberId,
		topic: topic, partitions: append([]int(nil), partitions...),
		persistCursor: persistCursor, handler: handler,
		pollingTime: time.Second, systemRetryConfig: DefaultRetryConfig(),
		processEventRetryConfig: DefaultRetryConfig(),
	}
	for _, option := range options {
		if option != nil {
			option(sp)
		}
	}
	return sp
}

func (sp *streamProcessor) validate() error {
	if sp.parent == nil || isNil(sp.eventStore) || sp.handler == nil {
		return fmt.Errorf("stream processor requires a context, event store, and handler")
	}
	if sp.topic == "" || (sp.persistCursor && sp.subscriberId == "") {
		return fmt.Errorf("stream processor requires a topic and a subscriber ID when persisting cursors")
	}
	if sp.pollingTime <= 0 || len(sp.partitions) == 0 {
		return fmt.Errorf("stream processor requires a positive polling interval and at least one partition")
	}
	seen := make(map[int]bool, len(sp.partitions))
	for _, partition := range sp.partitions {
		if partition < 0 || seen[partition] {
			return fmt.Errorf("invalid or duplicate partition: %d", partition)
		}
		seen[partition] = true
	}
	if err := sp.systemRetryConfig.validate(); err != nil {
		return fmt.Errorf("system retry: %w", err)
	}
	if err := sp.processEventRetryConfig.validate(); err != nil {
		return fmt.Errorf("event retry: %w", err)
	}
	return nil
}

func (sp *streamProcessor) Start() <-chan error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.run != nil {
		return sp.run.errors
	}
	if err := sp.validate(); err != nil {
		ch := make(chan error, 1)
		ch <- err
		close(ch)
		return ch
	}
	ctx, cancel := context.WithCancel(sp.parent)
	run := &processorRun{cancel: cancel, done: make(chan struct{}), errors: make(chan error, len(sp.partitions))}
	sp.run = run
	var workers sync.WaitGroup
	for _, partition := range sp.partitions {
		workers.Add(1)
		go func(partition int) {
			defer workers.Done()
			if err := sp.processPartition(ctx, partition); err != nil && ctx.Err() == nil {
				// Each worker can send at most once; the buffer holds one error
				// per worker, so shutdown never depends on an error consumer.
				run.errors <- err
				cancel()
			}
		}(partition)
	}
	go func() {
		workers.Wait()
		cancel()
		sp.mu.Lock()
		defer sp.mu.Unlock()
		sp.run = nil
		close(run.errors)
		close(run.done)
	}()
	return run.errors
}

func (sp *streamProcessor) Stop() {
	sp.mu.Lock()
	run := sp.run
	if run != nil {
		run.cancel()
	}
	sp.mu.Unlock()
	if run != nil {
		<-run.done
	}
}

func (sp *streamProcessor) processPartition(ctx context.Context, partition int) error {
	cursor := ""
	if sp.persistCursor {
		err := RetryWithExponentialBackoff(ctx, sp.systemRetryConfig, func() error {
			var err error
			cursor, err = sp.eventStore.GetSubscriptionCursor(ctx, sp.topic, sp.subscriberId, partition)
			if errors.Is(err, ErrCursorNotFound) {
				cursor = ""
				return nil
			}
			return err
		})
		if err != nil {
			return fmt.Errorf("load cursor for partition %d: %w", partition, err)
		}
	}
	ticker := time.NewTicker(sp.pollingTime)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			next, err := sp.processNewEvents(ctx, partition, cursor)
			if err != nil {
				return err
			}
			if sp.persistCursor && next != cursor {
				err = RetryWithExponentialBackoff(ctx, sp.systemRetryConfig, func() error {
					return sp.eventStore.SaveSubscriptionCursor(ctx, sp.topic, sp.subscriberId, partition, next)
				})
				if err != nil {
					return fmt.Errorf("save cursor for partition %d: %w", partition, err)
				}
			}
			cursor = next
		}
	}
}

func (sp *streamProcessor) processNewEvents(ctx context.Context, partition int, cursor string) (string, error) {
	var page EventsPage
	err := RetryWithExponentialBackoff(ctx, sp.systemRetryConfig, func() error {
		var err error
		page, err = sp.eventStore.GetEventsByTopic(ctx, sp.topic, partition, cursor)
		return err
	})
	if err != nil {
		return cursor, fmt.Errorf("fetch events for partition %d: %w", partition, err)
	}
	for _, event := range page.Events {
		if err := RetryWithExponentialBackoff(ctx, sp.processEventRetryConfig, func() error {
			return sp.processEvent(ctx, event)
		}); err != nil {
			return cursor, fmt.Errorf("process event for partition %d: %w", partition, err)
		}
	}
	return page.NextCursor, nil
}

func (sp *streamProcessor) processEvent(ctx context.Context, event Envelope) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic processing event: %v", recovered)
		}
	}()
	return sp.handler(ctx, event)
}

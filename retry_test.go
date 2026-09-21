package es

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryDefaultsAndValidation(t *testing.T) {
	cfg := DefaultRetryConfig()
	if cfg.MaxRetryDuration != 10*time.Second || cfg.InitialRetryDelay != 100*time.Millisecond {
		t.Fatalf("defaults: %+v", cfg)
	}
	for _, cfg := range []RetryConfig{{MaxRetryDuration: -1}, {InitialRetryDelay: -1}, {MaxRetryDuration: time.Second}} {
		if err := RetryWithExponentialBackoff(context.Background(), cfg, func() error { t.Error("invalid config ran operation"); return nil }); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	if err := RetryWithExponentialBackoff(nil, RetryConfig{}, func() error { return nil }); err == nil {
		t.Fatal("nil context accepted")
	}
	if err := RetryWithExponentialBackoff(context.Background(), RetryConfig{}, nil); err == nil {
		t.Fatal("nil operation accepted")
	}
}
func TestRetryAttemptsAndErrorPreservation(t *testing.T) {
	failure := errors.New("operation failure")
	calls := 0
	if err := RetryWithExponentialBackoff(context.Background(), RetryConfig{}, func() error { calls++; return nil }); err != nil || calls != 1 {
		t.Fatalf("success err=%v calls=%d", err, calls)
	}
	calls = 0
	err := RetryWithExponentialBackoff(context.Background(), RetryConfig{}, func() error { calls++; return failure })
	if !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("single attempt err=%v calls=%d", err, calls)
	}
	calls = 0
	err = RetryWithExponentialBackoff(context.Background(), RetryConfig{time.Second, time.Nanosecond}, func() error {
		calls++
		if calls < 3 {
			return failure
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("retry err=%v calls=%d", err, calls)
	}
}
func TestRetryBudgetExpiresDuringBackoff(t *testing.T) {
	failure := errors.New("permanent error")
	calls := 0
	err := RetryWithExponentialBackoff(context.Background(), RetryConfig{5 * time.Millisecond, time.Hour}, func() error { calls++; return failure })
	if !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("retry exceeded budget: err=%v calls=%d", err, calls)
	}
}
func TestRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := RetryWithExponentialBackoff(ctx, RetryConfig{}, func() error { t.Error("ran canceled operation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := RetryWithExponentialBackoff(ctx, RetryConfig{time.Hour, time.Hour}, func() error { calls++; cancel(); return errors.New("failed") })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancellation err=%v calls=%d", err, calls)
	}
}

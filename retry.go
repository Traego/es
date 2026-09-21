package es

import (
	"context"
	"fmt"
	"time"
)

// RetryConfig controls retries after an unsuccessful operation. A zero
// MaxRetryDuration permits one attempt. Otherwise InitialRetryDelay must be
// positive. The budget includes operation time, but cannot interrupt a blocking
// operation; operations must honor their own context.
type RetryConfig struct {
	MaxRetryDuration  time.Duration
	InitialRetryDelay time.Duration
}

// DefaultRetryConfig retries for up to ten seconds, initially waiting 100ms.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{MaxRetryDuration: 10 * time.Second, InitialRetryDelay: 100 * time.Millisecond}
}

func (config RetryConfig) validate() error {
	if config.MaxRetryDuration < 0 || config.InitialRetryDelay < 0 || (config.MaxRetryDuration > 0 && config.InitialRetryDelay == 0) {
		return fmt.Errorf("invalid retry configuration: durations must be nonnegative and retries require a positive initial delay")
	}
	return nil
}

// RetryWithExponentialBackoff calls operation until it succeeds, the retry budget
// expires, or ctx is canceled. The final operation error is preserved with %w.
// Cancellation prevents subsequent attempts and interrupts backoff waits.
func RetryWithExponentialBackoff(ctx context.Context, config RetryConfig, operation func() error) error {
	if ctx == nil || operation == nil {
		return fmt.Errorf("retry requires a context and operation")
	}
	if err := config.validate(); err != nil {
		return err
	}
	started := time.Now()
	delay := min(config.InitialRetryDelay, config.MaxRetryDuration)
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if lastErr != nil && time.Since(started) >= config.MaxRetryDuration {
			return fmt.Errorf("operation failed after retries: %w", lastErr)
		}
		lastErr = operation()
		if lastErr == nil {
			return nil
		}
		remaining := config.MaxRetryDuration - time.Since(started)
		if remaining <= 0 {
			return fmt.Errorf("operation failed after retries: %w", lastErr)
		}
		timer := time.NewTimer(min(delay, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		// Cap before doubling so even extreme durations cannot overflow.
		delay += min(delay, config.MaxRetryDuration-delay)
	}
}

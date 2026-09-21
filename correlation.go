package es

import "context"

type correlationKey struct{}

// WithCorrelationID associates a tracing identifier with aggregate writes.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID returns the tracing identifier, or an empty string if absent.
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

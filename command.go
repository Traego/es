package es

// Command interface for all org commands
type Command interface {
	TargetAggregateId() string
}

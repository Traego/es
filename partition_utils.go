package es

import (
	"errors"
	"github.com/spaolacci/murmur3"
)

// CalculatePartitionId deterministically maps a nonempty key to a partition.
// Counts must be positive and fit in an unsigned 32-bit integer. Changing a
// topic's count after writing events would change stream ownership.
func CalculatePartitionId(partitionKey string, partitionCount int) (int, error) {
	if partitionKey == "" {
		return 0, errors.New("partition key must be defined")
	}
	if partitionCount <= 0 || uint64(partitionCount) > 1<<32-1 {
		return 0, errors.New("partition count must be between 1 and 4294967295")
	}
	// hash.Hash.Write is specified to never return an error. The streaming
	// implementation avoids the uintptr arithmetic in murmur3.Sum32, which
	// fails Go's checkptr instrumentation.
	hasher := murmur3.New32WithSeed(0)
	_, _ = hasher.Write([]byte(partitionKey))
	return int(hasher.Sum32() % uint32(partitionCount)), nil
}

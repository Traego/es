package es

import (
	"context"
	"encoding/json"
	"fmt"
)

type JsonEventCodec struct {
}

func (j JsonEventCodec) Serialize(ctx context.Context, event Event) ([]byte, error) {
	res, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize event: %w", err)
	}

	return res, nil
}

func (j JsonEventCodec) Deserialize(ctx context.Context, data []byte, event Event) error {
	err := json.Unmarshal(data, event)
	if err != nil {
		return fmt.Errorf("failed to deserialize event: %w", err)
	}
	return nil
}

var _ EventCodec = (*JsonEventCodec)(nil)

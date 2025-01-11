package capture

import (
	"context"
)

type Capturer interface {
	Start() error

	Stop() error

	Events() <-chan *Event

	Checkpoint(ctx context.Context) (string, error)
	ACK(ctx context.Context, position string) error
}

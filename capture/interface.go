package capture

import (
	"context"

	"github.com/strahe/curio-sentinel/models"
)

type Capturer interface {
	Start() error

	Stop() error

	Events() <-chan *models.Event

	Checkpoint(ctx context.Context) (string, error)
	ACK(ctx context.Context, position string) error
}

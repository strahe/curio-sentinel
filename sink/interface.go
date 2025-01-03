package sink

import (
	"context"

	"github.com/strahe/curio-sentinel/models"
)

type Sink interface {
	Init(ctx context.Context, config map[string]any) error
	Write(ctx context.Context, events []*models.Event) error
	Flush(ctx context.Context) error
	Close() error
	Type() string
}

package sink

import (
	"context"

	"github.com/strahe/curio-sentinel/capturer"
)

type Sink interface {
	Write(ctx context.Context, events []*capturer.Event) error
	Flush(ctx context.Context) error
	Close() error
	Type() string
}

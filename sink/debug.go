package sink

import (
	"context"

	"github.com/strahe/curio-sentinel/models"
	"github.com/strahe/curio-sentinel/pkg/log"
)

type DebugSink struct{}

func NewDebugSink() *DebugSink {
	return &DebugSink{}
}

func (s *DebugSink) Init(ctx context.Context, config map[string]any) error {
	log.Debug().Msg("DebugSink Init")
	return nil
}

// Close implements Sink.
func (s *DebugSink) Close() error {
	log.Debug().Msg("DebugSink Close")
	return nil
}

// Flush implements Sink.
func (s *DebugSink) Flush(ctx context.Context) error {
	log.Debug().Msg("DebugSink Flush")
	return nil
}

// Type implements Sink.
func (s *DebugSink) Type() string {
	return "debug"
}

// Write implements Sink.
func (s *DebugSink) Write(ctx context.Context, events []models.Event) error {
	log.Debug().Msg("DebugSink Write")
	return nil
}

var _ Sink = (*DebugSink)(nil)

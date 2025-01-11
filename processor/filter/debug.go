package filter

import (
	"github.com/strahe/curio-sentinel/capture"
	"github.com/strahe/curio-sentinel/pkg/log"
	"github.com/strahe/curio-sentinel/processor"
)

type DebugFilter struct{}

func NewDebugFilter() *DebugFilter {
	return &DebugFilter{}
}

func (f *DebugFilter) Process(event *capture.Event) (*capture.Event, error) {
	log.Debug().Msgf("Filter event: %s: %s", event.ID, event.Type)
	return event, nil
}

var _ processor.EventProcessor = (*DebugFilter)(nil)

package filter

import (
	"github.com/strahe/curio-sentinel/capturer"
	"github.com/strahe/curio-sentinel/pkg/log"
	"github.com/strahe/curio-sentinel/processor"
)

type DebugFilter struct{}

func NewDebugFilter() *DebugFilter {
	return &DebugFilter{}
}

func (f *DebugFilter) Process(event *capturer.Event) (*capturer.Event, error) {
	log.Debug().Msgf("Filter event: %s: %s", event.ID, event.Type)
	return event, nil
}

var _ processor.EventProcessor = (*DebugFilter)(nil)

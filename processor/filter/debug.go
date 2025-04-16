package filter

import (
	"github.com/web3tea/curio-sentinel/capturer"
	"github.com/web3tea/curio-sentinel/processor"
)

type DebugFilter struct{}

func NewDebugFilter() *DebugFilter {
	return &DebugFilter{}
}

func (f *DebugFilter) Process(event *capturer.Event) (*capturer.Event, error) {
	log.Debugf("Filter event: %s: %s", event.ID, event.Type)
	return event, nil
}

var _ processor.EventProcessor = (*DebugFilter)(nil)

package transformer

import (
	"github.com/web3tea/curio-sentinel/capturer"
	"github.com/web3tea/curio-sentinel/processor"
)

type DebugTransformer struct{}

func NewDebugTransformer() *DebugTransformer {
	return &DebugTransformer{}
}

// Process implements processor.EventProcessor.
func (d *DebugTransformer) Process(event *capturer.Event) (*capturer.Event, error) {
	log.Debugf("Transform event: %s: %s", event.ID, event.Type)
	return event, nil
}

var _ processor.EventProcessor = (*DebugTransformer)(nil)

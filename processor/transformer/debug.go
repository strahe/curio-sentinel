package transformer

import (
	"github.com/strahe/curio-sentinel/capturer"
	"github.com/strahe/curio-sentinel/pkg/log"
	"github.com/strahe/curio-sentinel/processor"
)

type DebugTransformer struct{}

func NewDebugTransformer() *DebugTransformer {
	return &DebugTransformer{}
}

// Process implements processor.EventProcessor.
func (d *DebugTransformer) Process(event *capturer.Event) (*capturer.Event, error) {
	log.Debug().Msgf("Transformer event: %s: %s", event.ID, event.Type)
	return event, nil
}

var _ processor.EventProcessor = (*DebugTransformer)(nil)

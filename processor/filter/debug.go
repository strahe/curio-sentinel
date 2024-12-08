package filter

import (
	"github.com/strahe/curio-sentinel/models"
	"github.com/strahe/curio-sentinel/pkg/log"
	"github.com/strahe/curio-sentinel/processor"
)

type DebugFilter struct{}

func NewDebugFilter() *DebugFilter {
	return &DebugFilter{}
}

func (f *DebugFilter) Process(event *models.Event) (*models.Event, error) {
	log.Debug().Msgf("Filter event: %s: %s", event.ID, event.Type)
	return event, nil
}

var _ processor.EventProcessor = (*DebugFilter)(nil)

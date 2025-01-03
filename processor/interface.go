package processor

import (
	"github.com/strahe/curio-sentinel/models"
)

type EventProcessor interface {
	Process(event *models.Event) (*models.Event, error)
}

type ProcessorComposite interface {
	AddFilter(processor EventProcessor)
	AddTransformer(processor EventProcessor)
}

type Processor interface {
	EventProcessor
	ProcessorComposite
}

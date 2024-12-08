package processor

import (
	"github.com/strahe/curio-sentinel/models"
)

// EventProcessor 定义基本的事件处理能力
type EventProcessor interface {
	Process(event *models.Event) (*models.Event, error)
}

// ProcessorComposite 定义组合处理器的能力
type ProcessorComposite interface {
	AddFilter(processor EventProcessor)
	AddTransformer(processor EventProcessor)
}

type Processor interface {
	EventProcessor
	ProcessorComposite
}

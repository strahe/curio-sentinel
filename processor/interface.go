package processor

import "github.com/strahe/curio-sentinel/capture"

type EventProcessor interface {
	Process(event *capture.Event) (*capture.Event, error)
}

type ProcessorComposite interface {
	AddFilter(processor EventProcessor)
	AddTransformer(processor EventProcessor)
}

type Processor interface {
	EventProcessor
	ProcessorComposite
}

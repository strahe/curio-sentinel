package processor

import (
	"sync"

	"github.com/strahe/curio-sentinel/capturer"
)

type ProcessorChain struct {
	filterProcessor      []EventProcessor
	transformerProcessor []EventProcessor
	lk                   sync.Mutex
}

func NewProcessorChain() Processor {
	return &ProcessorChain{
		filterProcessor:      make([]EventProcessor, 0),
		transformerProcessor: make([]EventProcessor, 0),
	}
}

func (pc *ProcessorChain) Process(event *capturer.Event) (*capturer.Event, error) {
	currentEvent := event
	for _, p := range append(pc.filterProcessor, pc.transformerProcessor...) {
		processed, err := p.Process(currentEvent)
		if err != nil {
			return currentEvent, err
		}
		currentEvent = processed
	}
	return currentEvent, nil
}

func (pc *ProcessorChain) AddFilter(processor EventProcessor) {
	pc.lk.Lock()
	defer pc.lk.Unlock()

	pc.filterProcessor = append(pc.filterProcessor, processor)
}

func (pc *ProcessorChain) AddTransformer(processor EventProcessor) {
	pc.lk.Lock()
	defer pc.lk.Unlock()

	pc.transformerProcessor = append(pc.transformerProcessor, processor)
}

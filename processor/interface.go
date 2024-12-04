package processor

import (
	"github.com/strahe/curio-sentinel/models"
	"github.com/strahe/curio-sentinel/processor/filter"
	"github.com/strahe/curio-sentinel/processor/transformer"
)

type Processor interface {
	// Process 处理单个事件
	Process(event models.Event) (models.Event, error)

	// AddFilter 添加过滤器
	AddFilter(filter filter.Filter)

	// AddTransformer 添加转换器
	AddTransformer(transformer transformer.Transformer)
}

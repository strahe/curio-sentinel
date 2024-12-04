package transformer

import "github.com/strahe/curio-sentinel/models"

// Transformer 定义转换器接口
type Transformer interface {
	// Transform 对事件进行转换处理
	Transform(event models.Event) (models.Event, error)

	// Name 返回转换器名称
	Name() string
}

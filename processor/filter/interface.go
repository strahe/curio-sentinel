package filter

import "github.com/strahe/curio-sentinel/models"

type Filter interface {
	// Apply 应用过滤逻辑，返回是否保留该事件
	Apply(event models.Event) (bool, error)

	// Name 返回过滤器名称
	Name() string
}

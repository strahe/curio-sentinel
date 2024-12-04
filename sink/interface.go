package sink

import (
	"context"

	"github.com/strahe/curio-sentinel/models"
)

// Sink 定义数据输出目标接口
type Sink interface {
	// Init 初始化Sink
	Init(ctx context.Context, config map[string]any) error

	// Write 写入一组事件
	Write(ctx context.Context, events []models.Event) error

	// Flush 刷新缓冲的数据
	Flush(ctx context.Context) error

	// Close 关闭Sink
	Close() error

	// Type 返回Sink类型
	Type() string
}

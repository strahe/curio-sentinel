package capture

import (
	"github.com/strahe/curio-sentinel/models"
)

type Capturer interface {
	// Start 启动捕获进程
	Start() error

	// Stop 停止捕获进程
	Stop() error

	// Events 返回捕获的事件通道
	Events() <-chan models.Event

	// Checkpoint 获取/设置检查点位置
	Checkpoint() (string, error)
	SetCheckpoint(checkpoint string) error
}

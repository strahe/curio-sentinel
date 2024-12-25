package sentinel

import "time"

// Option 定义一个用于配置Sentinel的函数类型
type Option func(*Sentinel)

// WithCheckpointInterval 设置检查点间隔
func WithCheckpointInterval(interval time.Duration) Option {
	return func(s *Sentinel) {
		s.checkpointInterval = interval
	}
}

package sentinel

import "time"

// Option 定义一个用于配置Sentinel的函数类型
type Option func(*Sentinel)

// WithBatchSize 设置批处理大小
func WithBatchSize(size int) Option {
	return func(s *Sentinel) {
		if size > 0 {
			s.BatchSize = size
		}
	}
}

// WithFlushInterval 设置刷新间隔
func WithFlushInterval(interval time.Duration) Option {
	return func(s *Sentinel) {
		s.FlushInterval = interval
	}
}

// WithCheckpointInterval 设置检查点间隔
func WithCheckpointInterval(interval time.Duration) Option {
	return func(s *Sentinel) {
		s.checkpointInterval = interval
	}
}

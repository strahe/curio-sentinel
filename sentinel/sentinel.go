package sentinel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/strahe/curio-sentinel/capture"
	"github.com/strahe/curio-sentinel/processor"
	"github.com/strahe/curio-sentinel/sink"
)

// Status 表示Sentinel的运行状态
type Status string

const (
	StatusIdle     Status = "idle"
	StatusStarting Status = "starting"
	StatusRunning  Status = "running"
	StatusStopping Status = "stopping"
	StatusError    Status = "error"
)

// StatusReporter 用于报告状态变化
type StatusReporter interface {
	ReportStatus(status Status, message string)
}

type Sentinel struct {
	Capturer  capture.Capturer
	Processor processor.Processor
	Sink      sink.Sink

	BatchSize     int
	FlushInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	lastCheckpoint     string
	checkpointInterval time.Duration
	checkpointMu       sync.RWMutex

	statusReporter StatusReporter

	status   Status
	statusMu sync.RWMutex
}

func NewSentinel(capturer capture.Capturer, processor processor.Processor, sink sink.Sink, options ...Option) *Sentinel {
	s := &Sentinel{
		Capturer:           capturer,
		Processor:          processor,
		Sink:               sink,
		BatchSize:          1000,
		FlushInterval:      time.Second * 5,
		checkpointInterval: time.Minute,
	}

	// 应用选项
	for _, opt := range options {
		opt(s)
	}

	return s
}

// Start 启动监控和处理流程
func (s *Sentinel) Start(ctx context.Context) error {
	// 实现启动逻辑
	s.ctx, s.cancel = context.WithCancel(ctx)

	// 启动捕获器
	if err := s.Capturer.Start(s.ctx); err != nil {
		s.setStatus(StatusError)
		s.cancel()
		return fmt.Errorf("failed to start capturer: %w", err)
	}
	return nil
}

// Stop 停止监控和处理流程
func (s *Sentinel) Stop() error {
	// 实现停止逻辑
	panic("not implemented")
}

// setStatus 设置Sentinel状态
func (s *Sentinel) setStatus(status Status) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	s.status = status

	// 如果配置了状态报告器，通知状态变化
	if s.statusReporter != nil {
		s.statusReporter.ReportStatus(status, "")
	}
}

package sentinel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/strahe/curio-sentinel/capturer"
	"github.com/strahe/curio-sentinel/pkg/log"
	"github.com/strahe/curio-sentinel/processor"
	"github.com/strahe/curio-sentinel/sink"
)

type Status string

const (
	StatusIdle     Status = "idle"
	StatusStarting Status = "starting"
	StatusRunning  Status = "running"
	StatusStopping Status = "stopping"
	StatusError    Status = "error"
)

type StatusReporter interface {
	ReportStatus(status Status, message string)
}

type Sentinel struct {
	Capturer  capturer.Capturer
	Processor processor.Processor
	Sink      sink.Sink

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

func NewSentinel(capturer capturer.Capturer, processor processor.Processor, sink sink.Sink, options ...Option) *Sentinel {
	s := &Sentinel{
		Capturer:           capturer,
		Processor:          processor,
		Sink:               sink,
		checkpointInterval: time.Second,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

func (s *Sentinel) Start(ctx context.Context) (err error) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.setStatus(StatusStarting)
	defer func() {
		if err != nil {
			s.setStatus(StatusError)
			s.cancel()
		} else {
			s.setStatus(StatusRunning)
		}
	}()

	if err := s.Capturer.Start(); err != nil {
		return fmt.Errorf("failed to start capturer: %w", err)
	}

	s.wg.Add(1)
	go s.processEvents()

	return nil
}

func (s *Sentinel) Stop() (err error) {
	s.setStatus(StatusStopping)
	defer func() {
		if err == nil {
			s.setStatus(StatusIdle)
		} else {
			s.setStatus(StatusError)
		}
	}()
	if err := s.Capturer.Stop(); err != nil {
		log.Errorf("failed to stop capturer: %v", err)
	}
	s.cancel()
	s.wg.Wait()
	return nil
}

func (s *Sentinel) setStatus(status Status) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	s.status = status

	if s.statusReporter != nil {
		s.statusReporter.ReportStatus(status, "")
	}
}

func (s *Sentinel) processEvents() {
	defer s.wg.Done()

	events := s.Capturer.Events()
	checkpointTicker := time.NewTicker(s.checkpointInterval)
	defer checkpointTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			log.Infof("Event processing stopped due to context cancellation")
			return

		case event, ok := <-events:
			if !ok {
				log.Infof("Event channel closed, stopping event processing")
				return
			}

			processedEvent, err := s.Processor.Process(event)
			if err != nil {
				log.Errorf("Failed to process event: %v", err)
				continue
			}

			if processedEvent != nil {
				if err := s.Sink.Write(s.ctx, []*capturer.Event{processedEvent}); err != nil {
					log.Errorf("Failed to write event to sink: %v", err)
				} else {
					log.Debugf("Processed event: %v", processedEvent)
					s.updateLastCheckpoint(processedEvent.LSN)
				}
			}

		case <-checkpointTicker.C:
			s.updateCheckpoint(s.ctx)
		}
	}
}

func (s *Sentinel) updateLastCheckpoint(checkpoint string) {
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.lastCheckpoint = checkpoint
}

func (s *Sentinel) updateCheckpoint(ctx context.Context) {
	s.checkpointMu.RLock()
	checkpoint := s.lastCheckpoint
	s.checkpointMu.RUnlock()

	if checkpoint != "" {
		if err := s.Capturer.ACK(ctx, checkpoint); err != nil {
			log.Errorf("Failed to update checkpoint: %v", err)
		}
	}
}

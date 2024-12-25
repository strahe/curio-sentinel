package sentinel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/strahe/curio-sentinel/capture"
	"github.com/strahe/curio-sentinel/models"
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
	Capturer  capture.Capturer
	Processor processor.Processor
	Sink      sink.Sink

	// BatchSize     int
	// FlushInterval time.Duration

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
		checkpointInterval: time.Minute,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

func (s *Sentinel) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.Capturer.Start(); err != nil {
		s.setStatus(StatusError)
		s.cancel()
		return fmt.Errorf("failed to start capturer: %w", err)
	}

	s.wg.Add(1)
	go s.processEvents()

	s.setStatus(StatusRunning)
	return nil
}

func (s *Sentinel) Stop() error {
	defer s.setStatus(StatusStopping)
	if err := s.Capturer.Stop(); err != nil {
		log.Error().Err(err).Msg("failed to stop capturer")
	}
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
			log.Info().Msg("Event processing stopped due to context cancellation")
			return

		case event, ok := <-events:
			if !ok {
				log.Info().Msg("Event channel closed, stopping event processing")
				return
			}

			processedEvent, err := s.Processor.Process(event)
			if err != nil {
				log.Error().Err(err).Str("eventID", event.ID).Msg("Failed to process event")
				continue
			}

			if processedEvent != nil {
				if err := s.Sink.Write(s.ctx, []models.Event{*processedEvent}); err != nil {
					log.Error().Err(err).Str("eventID", processedEvent.ID).Msg("Failed to write event to sink")
				} else {
					log.Debug().Str("eventID", processedEvent.ID).Msg("Successfully processed and wrote event")
					s.updateLastCheckpoint(processedEvent.ID)
				}
			}

		case <-checkpointTicker.C:
			s.updateCheckpoint()
		}
	}
}

func (s *Sentinel) updateLastCheckpoint(checkpoint string) {
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.lastCheckpoint = checkpoint
}

func (s *Sentinel) updateCheckpoint() {
	s.checkpointMu.RLock()
	checkpoint := s.lastCheckpoint
	s.checkpointMu.RUnlock()

	if checkpoint != "" {
		if err := s.Capturer.SetCheckpoint(checkpoint); err != nil {
			log.Error().Err(err).Str("checkpoint", checkpoint).Msg("Failed to update checkpoint")
		} else {
			log.Debug().Str("checkpoint", checkpoint).Msg("Checkpoint updated")
		}
	}
}

package sentinel

import "time"

type Option func(*Sentinel)

func WithCheckpointInterval(interval time.Duration) Option {
	return func(s *Sentinel) {
		s.checkpointInterval = interval
	}
}

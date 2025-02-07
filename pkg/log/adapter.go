package log

import (
	"github.com/rs/zerolog"
	"github.com/strahe/curio-sentinel/capturer"
)

type ZerologAdapter struct {
	logger zerolog.Logger
}

func NewZerologAdapter(logger zerolog.Logger) capturer.Logger {
	return &ZerologAdapter{
		logger: logger,
	}
}

func (z *ZerologAdapter) Debugf(format string, args ...any) {
	z.logger.Debug().Msgf(format, args...)
}

func (z *ZerologAdapter) Infof(format string, args ...any) {
	z.logger.Info().Msgf(format, args...)
}

func (z *ZerologAdapter) Warnf(format string, args ...any) {
	z.logger.Warn().Msgf(format, args...)
}

func (z *ZerologAdapter) Errorf(format string, args ...any) {
	z.logger.Error().Msgf(format, args...)
}

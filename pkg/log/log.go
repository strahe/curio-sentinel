package log

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/rs/zerolog"
)

type ZeroLogger struct {
	logger zerolog.Logger
	name   string
}

func init() {
	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	// Set default to console writer for more readable logs during development
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()
	zerolog.DefaultContextLogger = &log
}

func SetGlobalLevel(level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
}

func NewLogger(name string, output io.Writer) *ZeroLogger {
	if output == nil {
		output = os.Stdout
	}

	// Create logger with caller information
	logger := zerolog.New(output).
		With().
		Timestamp().
		Str("logger", name).
		Caller(). // Enable file:line in logs
		Logger()

	return &ZeroLogger{
		logger: logger,
		name:   name,
	}
}

func (l *ZeroLogger) Debugf(format string, args ...any) {
	l.logger.Debug().Msgf(format, args...)
}

func (l *ZeroLogger) Infof(format string, args ...any) {
	l.logger.Info().Msgf(format, args...)
}

func (l *ZeroLogger) Warnf(format string, args ...any) {
	l.logger.Warn().Msgf(format, args...)
}

func (l *ZeroLogger) Errorf(format string, args ...any) {
	l.logger.Error().Msgf(format, args...)
}

var defaultLogger = NewLogger("default", nil)

func Debugf(format string, args ...any) {
	_, file, line, ok := runtime.Caller(1)
	event := defaultLogger.logger.Debug()
	if ok {
		event = event.Str("caller", filepath.Base(file)+":"+strconv.Itoa(line))
	}
	event.Msgf(format, args...)
}

func Infof(format string, args ...any) {
	_, file, line, ok := runtime.Caller(1)
	event := defaultLogger.logger.Info()
	if ok {
		event = event.Str("caller", filepath.Base(file)+":"+strconv.Itoa(line))
	}
	event.Msgf(format, args...)
}

func Warnf(format string, args ...any) {
	_, file, line, ok := runtime.Caller(1)
	event := defaultLogger.logger.Warn()
	if ok {
		event = event.Str("caller", filepath.Base(file)+":"+strconv.Itoa(line))
	}
	event.Msgf(format, args...)
}

func Errorf(format string, args ...any) {
	_, file, line, ok := runtime.Caller(1)
	event := defaultLogger.logger.Error()
	if ok {
		event = event.Str("caller", filepath.Base(file)+":"+strconv.Itoa(line))
	}
	event.Msgf(format, args...)
}

func Fatalf(format string, args ...any) {
	_, file, line, ok := runtime.Caller(1)
	event := defaultLogger.logger.Fatal()
	if ok {
		event = event.Str("caller", filepath.Base(file)+":"+strconv.Itoa(line))
	}
	event.Msgf(format, args...)
	// zerolog will call os.Exit(1) when the event is actually logged
}

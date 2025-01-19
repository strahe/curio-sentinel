package capturer

import (
	"context"
)

type Capturer interface {
	Start() error

	Stop() error

	Events() <-chan *Event

	Checkpoint(ctx context.Context) (string, error)
	ACK(ctx context.Context, position string) error
}

type DatabaseConfig struct {
	Hosts    []string `json:"hosts" yaml:"hosts" toml:"hosts"`
	Port     uint16   `json:"port" yaml:"port" toml:"port"`
	Username string   `json:"username" yaml:"username" toml:"username"`
	Password string   `json:"password" yaml:"password" toml:"password"`
	Database string   `json:"database" yaml:"database" toml:"database"`
}

type Config struct {
	Database              DatabaseConfig `json:"database" yaml:"database" toml:"database"`
	SlotName              string         `json:"slot_name" yaml:"slot_name" toml:"slot_name"`
	PublicationName       string         `json:"publication_name" yaml:"publication_name" toml:"publication_name"`
	Tables                []string       `json:"tables" yaml:"tables" toml:"tables"`
	DropSlotOnStop        bool           `json:"drop_slot_on_stop" yaml:"drop_slot_on_stop" toml:"drop_slot_on_stop"`
	DropPublicationOnStop bool           `json:"drop_publication_on_stop" yaml:"drop_publication_on_stop" toml:"drop_publication_on_stop"`
	EventBufferSize       int            `json:"event_buffer_size" yaml:"event_buffer_size" toml:"event_buffer_size"`
}

type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type noopLogger struct{}

func (l *noopLogger) Debugf(format string, args ...any) {}
func (l *noopLogger) Infof(format string, args ...any)  {}
func (l *noopLogger) Warnf(format string, args ...any)  {}
func (l *noopLogger) Errorf(format string, args ...any) {}

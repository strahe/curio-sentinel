package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/strahe/curio-sentinel/capturer"
	"github.com/strahe/curio-sentinel/pkg/log"
)

type StdoutSink struct {
	prettyPrint bool
}

func NewStdoutSink() *StdoutSink {
	return &StdoutSink{
		prettyPrint: true,
	}
}

func (s *StdoutSink) Init(ctx context.Context, config map[string]any) error {
	log.Debug().Msg("StdoutSink Init")

	// Check if pretty print is enabled in the config
	if prettyPrint, ok := config["pretty_print"].(bool); ok {
		s.prettyPrint = prettyPrint
	}

	return nil
}

func (s *StdoutSink) Close() error {
	log.Debug().Msg("StdoutSink Close")
	return nil
}

func (s *StdoutSink) Flush(ctx context.Context) error {
	log.Debug().Msg("StdoutSink Flush")
	return nil
}

func (s *StdoutSink) Type() string {
	return "stdout"
}

func (s *StdoutSink) Write(ctx context.Context, events []*capturer.Event) error {
	log.Debug().Msgf("StdoutSink Write %d events", len(events))

	if len(events) == 0 {
		return nil
	}

	if s.prettyPrint {
		output := s.buildPrettyOutput(events)
		fmt.Print(output)
	} else {
		var outputs []string
		for _, event := range events {
			data, err := json.Marshal(event)
			if err != nil {
				log.Error().Err(err).Msg("Failed to marshal event")
				continue
			}
			outputs = append(outputs, string(data))
		}
		fmt.Println(strings.Join(outputs, "\n"))
	}

	return nil
}

func (s *StdoutSink) printEvent(event *capturer.Event) {
	data, err := json.Marshal(event)
	if err != nil {
		fmt.Printf("Failed to marshal event: %v\n", err)
		return
	}
	fmt.Println(string(data))
}

func (s *StdoutSink) buildPrettyOutput(events []*capturer.Event) string {
	var sb strings.Builder

	for i, event := range events {
		if i > 0 {
			sb.WriteString("\n")
		}

		sb.WriteString("----------------------------------------\n")
		sb.WriteString(fmt.Sprintf("Event ID: %s\n", event.ID))
		sb.WriteString(fmt.Sprintf("Operation: %s\n", event.Type))
		sb.WriteString(fmt.Sprintf("Table: %s.%s\n", event.Schema, event.Table))
		sb.WriteString("PrimaryKey: \n")
		pkBytes, _ := json.MarshalIndent(event.PrimaryKey, "", "  ")
		sb.WriteString(string(pkBytes))
		sb.WriteString("\n")
		sb.WriteString(fmt.Sprintf("Timestamp: %s\n", event.Timestamp.Format(time.RFC3339)))
		if event.LSN != "" {
			sb.WriteString(fmt.Sprintf("LSN: %s\n", event.LSN))
		}

		sb.WriteString("Before:\n")
		dataBytes, _ := json.MarshalIndent(event.Before, "  ", "  ")
		sb.WriteString(string(dataBytes))
		sb.WriteString("\n")

		if len(event.Metadata) > 0 {
			sb.WriteString("Extra:\n")
			extraBytes, _ := json.MarshalIndent(event.Metadata, "  ", "  ")
			sb.WriteString(string(extraBytes))
			sb.WriteString("\n")
		}

		sb.WriteString("----------------------------------------\n")
	}

	return sb.String()
}

var _ Sink = (*StdoutSink)(nil)

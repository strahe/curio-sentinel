package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {
	AppName  string `json:"app_name" yaml:"app_name" toml:"app_name"`
	Version  string `json:"version" yaml:"version" toml:"version"`
	LogLevel string `json:"log_level" yaml:"log_level" toml:"log_level"`

	Capture CaptureConfig `json:"capture" yaml:"capture" toml:"capture"`

	Processor ProcessorConfig `json:"processor" yaml:"processor" toml:"processor"`

	Sink SinkConfig `json:"sink" yaml:"sink" toml:"sink"`
}

type DatabaseConfig struct {
	Hosts    []string `json:"hosts" yaml:"hosts" toml:"hosts"`
	Port     uint16   `json:"port" yaml:"port" toml:"port"`
	Username string   `json:"username" yaml:"username" toml:"username"`
	Password string   `json:"password" yaml:"password" toml:"password"`
	Database string   `json:"database" yaml:"database" toml:"database"`
}

type CaptureConfig struct {
	// Database
	Database DatabaseConfig `json:"database" yaml:"database" toml:"database"`

	SlotName              string   `json:"slot_name" yaml:"slot_name" toml:"slot_name"`
	PublicationName       string   `json:"publication_name" yaml:"publication_name" toml:"publication_name"`
	Tables                []string `json:"tables" yaml:"tables" toml:"tables"`
	DropSlotOnStop        bool     `json:"drop_slot_on_stop" yaml:"drop_slot_on_stop" toml:"drop_slot_on_stop"`
	DropPublicationOnStop bool     `json:"drop_publication_on_stop" yaml:"drop_publication_on_stop" toml:"drop_publication_on_stop"`
}

type ProcessorConfig struct {
	Filter               FilterConfig    `json:"filter" yaml:"filter" toml:"filter"`
	EnableTransformation bool            `json:"enable_transformation" yaml:"enable_transformation" toml:"enable_transformation"`
	TransformRules       []TransformRule `json:"transform_rules" yaml:"transform_rules" toml:"transform_rules"`
	MaxConcurrency       int             `json:"max_concurrency" yaml:"max_concurrency" toml:"max_concurrency"`
}

type FilterConfig struct {
	Types          []string `json:"types" yaml:"types" toml:"types"`
	Schemas        []string `json:"schemas" yaml:"schemas" toml:"schemas"`
	Tables         []string `json:"tables" yaml:"tables" toml:"tables"`
	ExcludeSchemas []string `json:"exclude_schemas" yaml:"exclude_schemas" toml:"exclude_schemas"`
	ExcludeTables  []string `json:"exclude_tables" yaml:"exclude_tables" toml:"exclude_tables"`
}

type TransformRule struct {
	SchemaPattern string            `json:"schema_pattern" yaml:"schema_pattern" toml:"schema_pattern"`
	TablePattern  string            `json:"table_pattern" yaml:"table_pattern" toml:"table_pattern"`
	ColumnName    string            `json:"column_name" yaml:"column_name" toml:"column_name"`
	Operation     string            `json:"operation" yaml:"operation" toml:"operation"` // rename, mask, format, etc.
	Parameters    map[string]string `json:"parameters" yaml:"parameters" toml:"parameters"`
}

type SinkConfig struct {
	Type string         `json:"type" yaml:"type" toml:"type"`
	File FileSinkConfig `json:"file,omitempty" yaml:"file,omitempty" toml:"file,omitempty"`
}

type FileSinkConfig struct {
	Path   string `json:"path" yaml:"path" toml:"path"`
	Format string `json:"format" yaml:"format" toml:"format"` // json, csv, avro, etc.
}

func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := DefaultConfig

	switch {
	case strings.HasSuffix(path, ".json"):
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	case strings.HasSuffix(path, ".toml"):
		if _, err := toml.Decode(string(data), &config); err != nil {
			return nil, fmt.Errorf("failed to parse TOML config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config file format: %s", path)
	}

	// todo: validateConfig

	return &config, nil
}

var DefaultConfig = Config{
	AppName:  "curio-sentinel",
	Version:  "0.1.0",
	LogLevel: "info",
	Capture:  CaptureConfig{},
}

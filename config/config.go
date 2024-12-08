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

	// 处理器配置
	Processor ProcessorConfig `json:"processor" yaml:"processor" toml:"processor"`

	// 接收器配置
	Sink SinkConfig `json:"sink" yaml:"sink" toml:"sink"`
}

// CaptureConfig 定义捕获器配置
type CaptureConfig struct {
	// 捕获器类型："yugabyte", "postgres", "mysql" 等
	Type string `json:"type" yaml:"type" toml:"type"`

	// 类型特定配置
	Yugabyte YugabyteConfig `json:"yugabyte,omitempty" yaml:"yugabyte,omitempty" toml:"yugabyte,omitempty"`

	// 通用配置
	BufferSize int `json:"buffer_size" yaml:"buffer_size" toml:"buffer_size"`
}

type YugabyteConfig struct {
	// 连接配置
	DSN string `json:"dsn" yaml:"dsn" toml:"dsn"`

	// 复制配置
	SlotName        string   `json:"slot_name" yaml:"slot_name" toml:"slot_name"`
	PublicationName string   `json:"publication_name" yaml:"publication_name" toml:"publication_name"`
	Tables          []string `json:"tables" yaml:"tables" toml:"tables"`
	DropSlotOnStop  bool     `json:"drop_slot_on_stop" yaml:"drop_slot_on_stop" toml:"drop_slot_on_stop"`
	TemporarySlot   bool     `json:"temporary_slot" yaml:"temporary_slot" toml:"temporary_slot"`

	// 协议配置
	ProtocolVersion string `json:"protocol_version" yaml:"protocol_version" toml:"protocol_version"` // 1 或 2
	EnableStreaming bool   `json:"enable_streaming" yaml:"enable_streaming" toml:"enable_streaming"`
	EnableMessages  bool   `json:"enable_messages" yaml:"enable_messages" toml:"enable_messages"`

	// 高级配置
	HeartbeatMs int64 `json:"heartbeat_ms" yaml:"heartbeat_ms" toml:"heartbeat_ms"`
}

// ProcessorConfig 定义处理器配置
type ProcessorConfig struct {
	// 处理器配置
	Filter               FilterConfig    `json:"filter" yaml:"filter" toml:"filter"`
	EnableTransformation bool            `json:"enable_transformation" yaml:"enable_transformation" toml:"enable_transformation"`
	TransformRules       []TransformRule `json:"transform_rules" yaml:"transform_rules" toml:"transform_rules"`
	MaxConcurrency       int             `json:"max_concurrency" yaml:"max_concurrency" toml:"max_concurrency"`
}

// FilterConfig 定义事件过滤配置
type FilterConfig struct {
	Types          []string `json:"types" yaml:"types" toml:"types"`
	Schemas        []string `json:"schemas" yaml:"schemas" toml:"schemas"`
	Tables         []string `json:"tables" yaml:"tables" toml:"tables"`
	ExcludeSchemas []string `json:"exclude_schemas" yaml:"exclude_schemas" toml:"exclude_schemas"`
	ExcludeTables  []string `json:"exclude_tables" yaml:"exclude_tables" toml:"exclude_tables"`
}

// TransformRule 定义数据转换规则
type TransformRule struct {
	SchemaPattern string            `json:"schema_pattern" yaml:"schema_pattern" toml:"schema_pattern"`
	TablePattern  string            `json:"table_pattern" yaml:"table_pattern" toml:"table_pattern"`
	ColumnName    string            `json:"column_name" yaml:"column_name" toml:"column_name"`
	Operation     string            `json:"operation" yaml:"operation" toml:"operation"` // rename, mask, format, etc.
	Parameters    map[string]string `json:"parameters" yaml:"parameters" toml:"parameters"`
}

// SinkConfig 定义接收器配置
type SinkConfig struct {
	// 接收器类型："kafka", "elasticsearch", "file", "database" 等
	Type string `json:"type" yaml:"type" toml:"type"`

	// 类型特定配置
	Kafka KafkaSinkConfig `json:"kafka,omitempty" yaml:"kafka,omitempty" toml:"kafka,omitempty"`
	File  FileSinkConfig  `json:"file,omitempty" yaml:"file,omitempty" toml:"file,omitempty"`
}

// KafkaSinkConfig 定义 Kafka 接收器配置
type KafkaSinkConfig struct {
	Brokers          []string          `json:"brokers" yaml:"brokers" toml:"brokers"`
	Topic            string            `json:"topic" yaml:"topic" toml:"topic"`
	KeyField         string            `json:"key_field" yaml:"key_field" toml:"key_field"`
	Compression      string            `json:"compression" yaml:"compression" toml:"compression"` // none, gzip, snappy, lz4
	Async            bool              `json:"async" yaml:"async" toml:"async"`
	BatchSize        int               `json:"batch_size" yaml:"batch_size" toml:"batch_size"`
	LingerMs         int               `json:"linger_ms" yaml:"linger_ms" toml:"linger_ms"`
	RetryMax         int               `json:"retry_max" yaml:"retry_max" toml:"retry_max"`
	RequiredAcks     int               `json:"required_acks" yaml:"required_acks" toml:"required_acks"`
	Partitioner      string            `json:"partitioner" yaml:"partitioner" toml:"partitioner"`    // hash, roundrobin, manual
	PartitionBy      string            `json:"partition_by" yaml:"partition_by" toml:"partition_by"` // schema, table
	CheckpointTopic  string            `json:"checkpoint_topic" yaml:"checkpoint_topic" toml:"checkpoint_topic"`
	SecurityProtocol string            `json:"security_protocol" yaml:"security_protocol" toml:"security_protocol"`
	SASLMechanism    string            `json:"sasl_mechanism" yaml:"sasl_mechanism" toml:"sasl_mechanism"`
	SASLUsername     string            `json:"sasl_username" yaml:"sasl_username" toml:"sasl_username"`
	SASLPassword     string            `json:"sasl_password" yaml:"sasl_password" toml:"sasl_password"`
	Options          map[string]string `json:"options" yaml:"options" toml:"options"`
}

// FileSinkConfig 定义文件接收器配置
type FileSinkConfig struct {
	Path             string `json:"path" yaml:"path" toml:"path"`
	Format           string `json:"format" yaml:"format" toml:"format"` // json, csv, avro, etc.
	RotationMaxBytes int64  `json:"rotation_max_bytes" yaml:"rotation_max_bytes" toml:"rotation_max_bytes"`
	RotationMaxTime  string `json:"rotation_max_time" yaml:"rotation_max_time" toml:"rotation_max_time"` // 1h, 24h, etc.
	Compression      string `json:"compression" yaml:"compression" toml:"compression"`                   // none, gzip, snappy
	SyncInterval     string `json:"sync_interval" yaml:"sync_interval" toml:"sync_interval"`
	CheckpointFile   string `json:"checkpoint_file" yaml:"checkpoint_file" toml:"checkpoint_file"`
}

// LoadFromFile 从文件加载配置
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := DefaultConfig

	// 根据文件扩展名决定解析方式
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

	// todo: validateConfig 验证配置

	return &config, nil
}

var DefaultConfig = Config{
	AppName:  "curio-sentinel",
	Version:  "0.1.0",
	LogLevel: "info",
}

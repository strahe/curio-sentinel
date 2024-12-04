package models

// Config 系统配置模型
type Config struct {
	Global    GlobalConfig    `yaml:"global"`
	Source    SourceConfig    `yaml:"source"`
	Processor ProcessorConfig `yaml:"processor"`
	Sinks     []SinkConfig    `yaml:"sinks"`
}

// GlobalConfig 全局配置
type GlobalConfig struct {
	LogLevel           string `yaml:"log_level"`
	MetricsAddress     string `yaml:"metrics_address"`
	AdminAPIAddress    string `yaml:"admin_api_address"`
	CheckpointInterval string `yaml:"checkpoint_interval"`
}

// SourceConfig 源配置
type SourceConfig struct {
	Type             string   `yaml:"type"`
	ConnectionString string   `yaml:"connection_string"`
	SlotName         string   `yaml:"slot_name"`
	PublicationName  string   `yaml:"publication_name"`
	Tables           []string `yaml:"tables"`
	ExcludeTables    []string `yaml:"exclude_tables"`
}

// ProcessorConfig 处理器配置
type ProcessorConfig struct {
	BatchSize       int              `yaml:"batch_size"`
	ParallelWorkers int              `yaml:"parallel_workers"`
	Filters         []map[string]any `yaml:"filters"`
	Transformers    []map[string]any `yaml:"transformers"`
}

// SinkConfig Sink配置
type SinkConfig struct {
	Name    string         `yaml:"name"`
	Type    string         `yaml:"type"`
	Enabled bool           `yaml:"enabled"`
	Config  map[string]any `yaml:"config"`
}

package config

type Config struct {
}

var DefaultConfig = Config{}

func NewFromFile(path string) (*Config, error) {
	return &DefaultConfig, nil
}

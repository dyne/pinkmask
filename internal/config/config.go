package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	IncludeTables []string                `yaml:"include_tables"`
	ExcludeTables []string                `yaml:"exclude_tables"`
	Tables        map[string]*TableConfig `yaml:"tables"`
	Subset        *SubsetConfig           `yaml:"subset"`
}

type TableConfig struct {
	Columns map[string]*TransformConfig `yaml:"columns"`
	Limit   int                         `yaml:"limit"`
	Where   string                      `yaml:"where"`
}

type TransformConfig struct {
	Type        string            `yaml:"type"`
	Params      map[string]any    `yaml:"params"`
	Value       any               `yaml:"value"`
	Pattern     string            `yaml:"pattern"`
	Replace     string            `yaml:"replace"`
	Locale      string            `yaml:"locale"`
	MaxLen      int               `yaml:"maxlen"`
	Map         map[string]string `yaml:"map"`
	LookupTable string            `yaml:"lookup_table"`
	LookupKey   string            `yaml:"lookup_key"`
	LookupValue string            `yaml:"lookup_value"`
}

type SubsetConfig struct {
	Roots []RootConfig `yaml:"roots"`
}

type RootConfig struct {
	Table string `yaml:"table"`
	Where string `yaml:"where"`
	Limit int    `yaml:"limit"`
}

func Load(path string) (*Config, error) {
	if path == "" {
		return &Config{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

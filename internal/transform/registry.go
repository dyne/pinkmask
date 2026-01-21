package transform

import (
	"fmt"
	"strings"

	"github.com/dyne/pinkmask/internal/config"
)

type PluginFunc func(value any, row map[string]any) (any, error)

type Factory func(cfg *config.TransformConfig, salt string) (Transformer, error)

var registry = map[string]Factory{}

func Register(name string, factory Factory) {
	if name == "" || factory == nil {
		return
	}
	registry[strings.ToLower(name)] = factory
}

func registerPlugin(name string, fn PluginFunc) {
	Register(name, func(cfg *config.TransformConfig, salt string) (Transformer, error) {
		return &PluginTransformer{name: name, fn: fn, cfg: cfg}, nil
	})
}

type PluginTransformer struct {
	name string
	fn   PluginFunc
	cfg  *config.TransformConfig
}

func (t *PluginTransformer) Name() string { return t.name }

func (t *PluginTransformer) Transform(value any, row RowContext) (any, error) {
	if t.fn == nil {
		return nil, fmt.Errorf("plugin transformer %s not initialized", t.name)
	}
	ctx := map[string]any{
		"table": row.Table,
		"pk":    row.PK,
		"seed":  row.Seed,
		"salt":  row.Salt,
		"config": map[string]any{
			"type":         cfgString(t.cfg, "Type"),
			"params":       cfgParams(t.cfg),
			"value":        cfgValue(t.cfg),
			"pattern":      cfgString(t.cfg, "Pattern"),
			"replace":      cfgString(t.cfg, "Replace"),
			"locale":       cfgString(t.cfg, "Locale"),
			"maxlen":       cfgMaxLen(t.cfg),
			"map":          cfgMap(t.cfg),
			"lookup_table": cfgString(t.cfg, "LookupTable"),
			"lookup_key":   cfgString(t.cfg, "LookupKey"),
			"lookup_value": cfgString(t.cfg, "LookupValue"),
		},
	}
	return t.fn(value, ctx)
}

func cfgString(cfg *config.TransformConfig, field string) string {
	if cfg == nil {
		return ""
	}
	switch field {
	case "Type":
		return cfg.Type
	case "Pattern":
		return cfg.Pattern
	case "Replace":
		return cfg.Replace
	case "Locale":
		return cfg.Locale
	case "LookupTable":
		return cfg.LookupTable
	case "LookupKey":
		return cfg.LookupKey
	case "LookupValue":
		return cfg.LookupValue
	default:
		return ""
	}
}

func cfgParams(cfg *config.TransformConfig) map[string]any {
	if cfg == nil || cfg.Params == nil {
		return map[string]any{}
	}
	return cfg.Params
}

func cfgValue(cfg *config.TransformConfig) any {
	if cfg == nil {
		return nil
	}
	return cfg.Value
}

func cfgMaxLen(cfg *config.TransformConfig) int {
	if cfg == nil {
		return 0
	}
	return cfg.MaxLen
}

func cfgMap(cfg *config.TransformConfig) map[string]string {
	if cfg == nil || cfg.Map == nil {
		return map[string]string{}
	}
	return cfg.Map
}

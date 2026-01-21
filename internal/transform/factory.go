package transform

import (
	"fmt"
	"strings"

	"github.com/dyne/pinkmask/internal/config"
)

func Build(cfg *config.TransformConfig, salt string) (Transformer, error) {
	if cfg == nil {
		return nil, nil
	}
	key := strings.ToLower(cfg.Type)
	if factory, ok := registry[key]; ok {
		return factory(cfg, salt)
	}
	switch key {
	case "hashsha256":
		return NewHashSha256(salt, cfg.MaxLen), nil
	case "hmacsha256":
		return NewHmacSha256(salt, cfg.MaxLen), nil
	case "stabletokenize":
		return NewStableTokenize(cfg.MaxLen), nil
	case "regexreplace":
		return NewRegexReplace(cfg.Pattern, cfg.Replace)
	case "setnull":
		return &SetNull{}, nil
	case "setvalue":
		return NewSetValue(cfg.Value), nil
	case "fakername":
		return &FakerName{}, nil
	case "fakeremail":
		return &FakerEmail{}, nil
	case "fakeraddress":
		return &FakerAddress{}, nil
	case "fakerphone":
		return &FakerPhone{}, nil
	case "dateshift":
		maxDays := 30
		if cfg.Params != nil {
			if v, ok := cfg.Params["max_days"]; ok {
				if iv, ok := asInt(v); ok {
					maxDays = iv
				}
			}
		}
		return NewDateShift(maxDays), nil
	case "map":
		return NewMapReplace(cfg.Map), nil
	default:
		return nil, fmt.Errorf("unknown transformer type: %s", cfg.Type)
	}
}

func asInt(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case float32:
		return int(t), true
	default:
		return 0, false
	}
}

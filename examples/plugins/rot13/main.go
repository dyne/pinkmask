package main

func rot13(input string) string {
	out := make([]byte, len(input))
	for i := 0; i < len(input); i++ {
		b := input[i]
		switch {
		case b >= 'a' && b <= 'z':
			out[i] = 'a' + (b-'a'+13)%26
		case b >= 'A' && b <= 'Z':
			out[i] = 'A' + (b-'A'+13)%26
		default:
			out[i] = b
		}
	}
	return string(out)
}

var Transformers = map[string]func(any, map[string]any) (any, error){
	"Rot13": func(value any, ctx map[string]any) (any, error) {
		if value == nil {
			return nil, nil
		}
		s, ok := value.(string)
		if !ok {
			return value, nil
		}
		return rot13(s), nil
	},
}

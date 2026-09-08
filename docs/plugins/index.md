# Plugins

Pinkmask supports optional Go plugins to keep the core binary lean while enabling high-performance custom transforms and external dependencies. Plugins are loaded via `--plugin` and register transformer names used in config.

::: warning Platform
Go plugins work on Linux and Darwin only.
:::

## Writing a transformer

1. Create a Go module (or a simple `main` package) that builds as a plugin.
2. Export a `Transformers` symbol: a map from transformer name to a function.
3. Use the transformer name in your YAML config (`type: YourName`).

Function signature: `func(any, map[string]any) (any, error)`

- `value` is the column value (possibly `nil`)
- `ctx` includes `table`, `pk`, `seed`, `salt`, and `config` (map of transformer config fields)

```go
package main

import "strings"

var Transformers = map[string]func(any, map[string]any) (any, error){
	"Lowercase": func(value any, ctx map[string]any) (any, error) {
		s, ok := value.(string)
		if !ok {
			return value, nil
		}
		return strings.ToLower(s), nil
	},
}
```

## Build

```bash
go build -buildmode=plugin -o lowercase.${GOOS}.${GOARCH}.so .
```

## Use

```yaml
tables:
  users:
    columns:
      email:
        type: Lowercase
```

```bash
pinkmask copy --in input.sqlite --out output.sqlite --config mask.yml --plugin ./lowercase.so
```

## Resolution

Passing `--plugin ./rot13` resolves, in order:

- `./rot13.<goos>.<goarch>.so`
- `./rot13.<goarch>.so`
- `./rot13.so`

Passing a directory loads every compatible plugin inside it (filenames ending in `.<goos>.<goarch>.so`, `.<goarch>.so`, or `.so`).

Example plugin source: `examples/plugins/rot13/main.go`

```bash
go build -buildmode=plugin -o rot13.so ./examples/plugins/rot13
```

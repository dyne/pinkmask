# pinkmask

Deterministic SQLite anonymization and subsetting tool inspired by Greenmask.

## Install

```bash
go install github.com/dyne/pinkmask/cmd/pinkmask@latest
```

## Usage

```bash
pinkmask copy --in input.sqlite --out output.sqlite --config examples/mask.yml --salt "abc" --seed 1
pinkmask sample --in input.sqlite --out output.sqlite --config examples/mask.yml --salt "abc" --seed 1
pinkmask inspect --in input.sqlite
pinkmask plan --in input.sqlite --config examples/mask.yml
pinkmask inspect --in input.sqlite --draft-config mask.draft.yml
```

## Config reference

Config file is YAML. Example at `examples/mask.yml`.

Inspect draft config:
- `pinkmask inspect --in input.sqlite --draft-config mask.draft.yml`
- Uses PII name heuristics to emit a starter `mask.yml` with suggested transformers.

### Schema handling

Pinkmask copies SQLite schema objects in this order:
- Tables (from `sqlite_master`)
- Data rows
- Views, indexes, and triggers (optional via `--triggers on|off`)

Schema introspection uses:
- `PRAGMA table_info(table)` for columns and primary keys
- `PRAGMA foreign_key_list(table)` for FK graph ordering
- `sqlite_master` for SQL definitions of tables/views/indexes/triggers

By default, data copy is ordered by primary key (or `rowid`) and tables are ordered by foreign-key dependencies.

Top-level:
- `include_tables`: list of glob patterns to include
- `exclude_tables`: list of glob patterns to exclude
- `tables`: per-table column transforms
- `subset`: graph-aware subsetting configuration

Transformers:
- `HashSha256` (salted) with optional `maxlen`
- `HmacSha256` (salt as key) with optional `maxlen`
- `StableTokenize` (short base32 token) with optional `maxlen`
- `RegexReplace` (`pattern`, `replace`)
- `SetNull`
- `SetValue` (`value`)
- `FakerName`, `FakerEmail`, `FakerAddress`, `FakerPhone` (deterministic)
- `DateShift` (`params.max_days`)
- `Map` (`map` inline or `lookup_table`, `lookup_key`, `lookup_value`)

## Plugins (fast custom transformers)

Pinkmask supports optional Go plugins to keep the core binary lean while enabling high-performance, custom transforms and external dependencies. Plugins are loaded via `--plugin` and can register transformer names used in config.

### Writing a new transformer (plugin)

1) Create a Go module (or use a simple `main` package) that builds as a plugin.
2) Export a `Transformers` symbol: a map from transformer name to a function.
3) Use the transformer name in your YAML config (`type: YourName`).

Function signature:
- `func(any, map[string]any) (any, error)`
- `value` is the column value (possibly `nil`)
- `ctx` includes `table`, `pk`, `seed`, `salt`, and `config`

Example:

```go
package main

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

Build:

```bash
go build -buildmode=plugin -o lowercase.${GOOS}.${GOARCH}.so .
```

Config usage:

```yaml
tables:
  users:
    columns:
      email:
        type: Lowercase
```

Plugin contract (Linux/Darwin only):
- Build a Go plugin (`.so`) exporting a `Transformers` symbol:
  - `var Transformers = map[string]func(any, map[string]any) (any, error){ ... }`
- Each function receives the column value plus a context map with:
  - `table`, `pk`, `seed`, `salt`, and `config` (map of transformer config fields).

Example plugin skeleton:

```go
package main

var Transformers = map[string]func(any, map[string]any) (any, error){
	"MyFastMask": func(value any, ctx map[string]any) (any, error) {
		return value, nil
	},
}
```

Usage:

```bash
pinkmask copy --in input.sqlite --out output.sqlite --config examples/mask.yml --plugin ./myplugin.so
```

Example plugin source: `examples/plugins/rot13/main.go`
Build locally:

```bash
go build -buildmode=plugin -o rot13.so ./examples/plugins/rot13
```

Architecture-specific plugin naming:
- You can pass `--plugin ./rot13` and pinkmask will resolve:
  - `./rot13.<goos>.<goarch>.so`
  - `./rot13.<goarch>.so`
  - `./rot13.so` (if it already exists)

Directory loading:
- You can pass a directory to `--plugin` to load all compatible plugins inside it.
- Compatible filenames end with:
  - `.<goos>.<goarch>.so`
  - `.<goarch>.so`
  - `.so`

Subset example:

```yaml
subset:
  roots:
    - table: users
      where: "country = 'US'"
      limit: 50
```

### mask.yml schema

```yaml
include_tables:
  - "users*"
exclude_tables:
  - "audit_*"

tables:
  users:
    columns:
      email:
        type: HmacSha256
        maxlen: 24
      full_name:
        type: FakerName
      ssn:
        type: SetNull
      note:
        type: RegexReplace
        pattern: "[0-9]+"
        replace: "X"
      status:
        type: Map
        map:
          active: active
          inactive: inactive
  orders:
    columns:
      shipping_address:
        type: FakerAddress

subset:
  roots:
    - table: users
      where: "country = 'US'"
      limit: 50
```

#### Table config

- `tables.<table>.columns.<column>`: transformer config for a column
- `tables.<table>.where`: optional filter for root subsetting (used by `sample`)
- `tables.<table>.limit`: optional limit for root subsetting (used by `sample`)

#### Transformer config fields

- `type`: transformer name (built-in or plugin)
- `params`: map of transformer-specific params (e.g., `max_days`)
- `value`: static value for `SetValue`
- `pattern`, `replace`: for `RegexReplace`
- `locale`: reserved (currently `en` only)
- `maxlen`: optional max output length for hash/token transforms
- `map`: inline mapping dictionary for `Map`
- `lookup_table`, `lookup_key`, `lookup_value`: database lookup mapping for `Map`

#### Subset config

- `subset.roots`: list of roots to seed graph-aware subsetting
- `subset.roots[].table`: root table name
- `subset.roots[].where`: SQL WHERE clause for root selection
- `subset.roots[].limit`: limit on root selection

## Demo

```bash
go run examples/make_demo_db.go demo.sqlite
pinkmask copy --in demo.sqlite --out anon.sqlite --config examples/mask.yml --salt "abc" --seed 1
```

Docker demo:

```bash
docker compose up
```

This produces `demo.sqlite`, `anon.sqlite`, and `anon_users.csv` in the repo.

## Limitations

- SQLite only; no external SQL parser.
- Triggers and views are copied as-is and may have side effects during import.
- For tables without primary keys, deterministic per-row values are derived from a row fingerprint.
- Subsetting expands selections via foreign keys; complex custom join logic is not supported.
- Built-in faker coverage is intentionally small; use plugins for large catalogs or specialized generators.

## Development

```bash
task fmt
task test
task demo
```

## Acknowledgments

- Idea and architecture: Puria Nafisi Azizi.
- AI-assisted development.

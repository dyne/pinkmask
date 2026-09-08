# mask.yml reference

Config is YAML. Example at `examples/mask.yml`.

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

## Top-level

| Key | Meaning |
| --- | --- |
| `include_tables` | List of glob patterns to include |
| `exclude_tables` | List of glob patterns to exclude |
| `tables` | Per-table column transforms |
| `subset` | Graph-aware subsetting configuration |

## Table config

- `tables.<table>.columns.<column>` — transformer config for a column
- `tables.<table>.where` — optional filter for root subsetting (used by `sample`)
- `tables.<table>.limit` — optional limit for root subsetting (used by `sample`)

## Transformer config fields

| Field | Meaning |
| --- | --- |
| `type` | Transformer name (built-in or plugin) |
| `params` | Map of transformer-specific params (e.g. `max_days`) |
| `value` | Static value for `SetValue` |
| `pattern`, `replace` | For `RegexReplace` |
| `locale` | Reserved (currently `en` only) |
| `maxlen` | Optional max output length for hash/token transforms |
| `map` | Inline mapping dictionary for `Map` |
| `lookup_table`, `lookup_key`, `lookup_value` | Database lookup mapping for `Map` |

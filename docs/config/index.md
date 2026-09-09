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

seed_rows:
  _superusers:
    - email: "pinkmask-admin@example.com"
      password: "$2a$10$GDzXh6uguwvZMBxIOjsTK.UXZazOoT07yUciJr3cEX3Kqw6e8P4fy"

## Top-level

| Key | Meaning |
| --- | --- |
| `include_tables` | List of glob patterns to include |
| `exclude_tables` | List of glob patterns to exclude |
| `tables` | Per-table column transforms |
| `seed_rows` | Extra rows to insert after the copy (e.g. a known login) |
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
| `locale` | Reserved (currently `en` only) |
| `maxlen` | Optional max output length for hash/token transforms |
| `map` | Inline mapping dictionary for `Map` |
| `lookup_table`, `lookup_key`, `lookup_value` | Database lookup mapping for `Map` |

## Seed rows

`seed_rows` inserts rows into the destination after the data copy and before
indexes/triggers are created. Each entry maps a table name to a list of rows;
each row is a map of column names to values. Columns are validated against the
source schema and values are inserted verbatim. It is the intended way to
create a known login when passwords have been redacted.

PocketBase stores auth passwords as bcrypt hashes (`$2a$10$…`, 60 chars), so a
seeded `_superusers` row must carry a bcrypt hash, not a plaintext password.
With Apache `htpasswd` installed:

```bash
htpasswd -bnBC 10 "" 'change-me' | tr -d ':\n'
```

The example above seeds a superuser whose password is the bcrypt hash of
`pinkmask-admin-password` (cost 10). Change the password before using it.

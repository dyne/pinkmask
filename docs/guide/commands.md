# Commands

`pinkmask` operates on SQLite files and keeps masking deterministic when the same salt and seed are reused.

```bash
pinkmask copy    --in input.sqlite --out output.sqlite --config examples/mask.yml --salt "abc" --seed 1
pinkmask sample  --in input.sqlite --out output.sqlite --config examples/mask.yml --salt "abc" --seed 1
pinkmask inspect --in input.sqlite
pinkmask plan    --in input.sqlite --config examples/mask.yml
pinkmask inspect --in input.sqlite --draft-config mask.draft.yml
```

| Command | What it does |
| --- | --- |
| `copy` | Copy the whole database, applying column transforms from the config. |
| `sample` | Copy and mask only the graph-aware subset defined under `subset`. |
| `inspect` | Print tables, columns, keys, and likely PII columns. With `--draft-config`, emit a starter `mask.yml`. |
| `plan` | Show the tables and transforms that `copy` would use without writing anything. |

## Common flags

These persistent flags apply to every command:

- `--salt` — secret used by hash, HMAC, and token transformers.
- `--seed` — integer seed for faker and date-shift transformers.
- `--fk on|off` — foreign-key enforcement during the copy (default `on`).
- `--triggers on|off` — whether to copy triggers (default `on`).
- `--jobs` — copy parallelism (default `4`).
- `--tempdir` — directory for temporary copy files.
- `--plugin path` — load a Go plugin (`.so`) or a directory of plugins; repeat for multiple plugins. See [Plugins](/plugins/).
- `--verbose` — enable debug logging.

`copy` and `sample` require `--in` and `--out`. `plan` requires `--in`; `copy`, `sample`, and `plan` accept an optional `--config` path. `inspect` requires `--in` and accepts `--draft-config` (`-` writes YAML to stdout).

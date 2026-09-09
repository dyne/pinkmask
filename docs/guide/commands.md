# Commands

`pinkmask` operates on SQLite files and PocketBase instances. Masking stays deterministic when the same salt and seed are reused.

```bash
pinkmask copy    --in input.sqlite --out output.sqlite --config examples/mask.yml --salt "abc" --seed 1
pinkmask sample  --in input.sqlite --out output.sqlite --config examples/mask.yml --salt "abc" --seed 1
pinkmask inspect --in input.sqlite
pinkmask plan    --in input.sqlite --config examples/mask.yml
pinkmask inspect --in input.sqlite --draft-config mask.draft.yml
pinkmask copy    --in pb:https://source.example --out ./output.sqlite --config examples/mask.yml --source-email "$PB_SOURCE_EMAIL" --source-password "$PB_SOURCE_PASSWORD"
pinkmask copy    --in ./input.sqlite --out pb:https://masked.example --config examples/mask.yml --dest-email "$PB_DEST_EMAIL" --dest-password "$PB_DEST_PASSWORD"
pinkmask pb      --source-url https://source.example --dest-url https://masked.example --config examples/pocketbase-mask.yml --salt "abc" --seed 1
pinkmask version
```

| Command | What it does |
| --- | --- |
| `copy` | Copy SQLite databases, transfer SQLite to PocketBase or PocketBase to SQLite when an endpoint uses the `pb:` URL prefix, and apply configured transforms. |
| `sample` | Copy and mask only the graph-aware subset defined under `subset`. |
| `inspect` | Print tables, columns, keys, and likely PII columns. With `--draft-config`, emit a starter `mask.yml`. |

| `plan` | Show the tables and transforms that `copy` would use without writing anything. |
| `pb` | Copy between PocketBase instances, creating missing collections and applying configured transforms. |
| `version` | Print the version. `pinkmask --version` works too. |

`inspect --draft-config` uses column names, SQLite `NOT NULL` and `UNIQUE` constraints, and PocketBase metadata conventions. It leaves collection schema fields such as `_collections.name`, rules, indexes, IDs, timestamps, and boolean flags unchanged; required sensitive fields use a non-null redaction, and unique PII fields use deterministic HMAC values instead of a small faker pool. Treat the draft as a reviewed starting point, especially for application-specific fields.

For `copy`, use `pb:<url>` in either `--in` or `--out`, but not both. The
other endpoint must be a local SQLite path. SQLite foreign keys become
PocketBase relation fields when importing into PocketBase. PocketBase relations
become SQLite foreign-key columns when exporting to SQLite. PB credentials are
read from `--source-email`/`--source-password` or `--dest-email`/`--dest-password`
and their `PB_*` environment variables.

`copy` and `sample` (for SQLite-to-SQLite copies) also honour the `seed_rows`
config key: rows listed there are inserted into the destination after the data
copy, before indexes and triggers. This is how you add a known login when
passwords have been redacted — seed a `_superusers` row whose `password` is a
bcrypt hash. See [mask.yml reference](/config/) for details and an example.

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

## PocketBase credentials

The `pb` command accepts `--source-email`, `--source-password`, `--dest-email`,
and `--dest-password` when the corresponding endpoint is a URL. The equivalent
environment variables are `PB_SOURCE_EMAIL`, `PB_SOURCE_PASSWORD`,
`PB_DEST_EMAIL`, and `PB_DEST_PASSWORD`; environment values take precedence over
flags. Existing destination collections are left unchanged; only missing
collections are created from the source schema.

The repository includes a seeded source fixture at
`testdata/pocketbase/source/pb_data`. Run the two-instance Docker test with:

```bash
docker compose --profile pb up --abort-on-container-exit \
  --exit-code-from pinkmask-pb \
  pb-source pb-destination pb-seed pinkmask-pb
```

The source and destination are mounted as volumes on ports `8090` and `8091`.
The fixture superuser is `admin@example.com` with password
`pinkmask-admin-password`. Reset the disposable destination volume with
`docker compose --profile pb down -v`.

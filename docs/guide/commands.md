# Commands

`pinkmask` operates on SQLite files and PocketBase instances. Masking stays deterministic when the same salt and seed are reused.

```bash
pinkmask copy    --in input.sqlite --out output.sqlite --config examples/mask.yml --salt "abc" --seed 1
pinkmask sample  --in input.sqlite --out output.sqlite --config examples/mask.yml --salt "abc" --seed 1
pinkmask inspect --in input.sqlite
pinkmask plan    --in input.sqlite --config examples/mask.yml
pinkmask inspect --in input.sqlite --draft-config mask.draft.yml
pinkmask pb      --source-url https://source.example --dest-url https://masked.example --config examples/pocketbase-mask.yml --salt "abc" --seed 1
pinkmask version
```

| Command | What it does |
| --- | --- |
| `pb` | Copy all included non-system, writable collections from one PocketBase instance to another, creating missing schemas and applying column transforms. Source records are not modified; IDs are preserved, auth passwords are randomized, file fields receive masked placeholders, and read-only views are skipped. |
| `copy` | Copy the whole database, applying column transforms from the config. |
| `sample` | Copy and mask only the graph-aware subset defined under `subset`. |
| `inspect` | Print tables, columns, keys, and likely PII columns. With `--draft-config`, emit a starter `mask.yml`. |
| `plan` | Show the tables and transforms that `copy` would use without writing anything. |
| `version` | Print the version. `pinkmask --version` works too. |

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
and `--dest-password`. The equivalent environment variables are
`PB_SOURCE_EMAIL`, `PB_SOURCE_PASSWORD`, `PB_DEST_EMAIL`, and
`PB_DEST_PASSWORD`; environment values take precedence over flags. `--source-url`
and `--dest-url` are required. Existing destination collections are left
unchanged; only missing non-system writable collections are created from the
source schema.

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

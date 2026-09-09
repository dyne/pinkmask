# Getting started

## Install

```bash
go install github.com/dyne/pinkmask/cmd/pinkmask@latest
```

## First run

Generate a demo database, then produce an anonymized copy:

```bash
go run examples/make_demo_db.go demo.sqlite
pinkmask copy --in demo.sqlite --out anon.sqlite --config examples/mask.yml --salt "abc" --seed 1
```

::: tip Deterministic by design
The same `--salt` and `--seed` always yield the same output. Change either to get a fresh mapping.
:::

## Draft a config from your schema

```bash
pinkmask inspect --in input.sqlite --draft-config mask.draft.yml
```

`inspect` uses PII name heuristics to emit a starter `mask.yml` with suggested transformers. Review it, rename to `mask.yml`, and run `copy`.

## Docker demo

```bash
docker compose up
```

Produces `demo.sqlite`, `anon.sqlite`, and `anon_users.csv` in the repo.

## Development

```bash
mise run fmt
mise run test
mise run demo
```

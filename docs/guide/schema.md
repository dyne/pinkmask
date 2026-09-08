# Schema handling

Pinkmask copies SQLite schema objects in this order:

1. Tables (from `sqlite_master`)
2. Data rows
3. Views, indexes, and triggers (optional via `--triggers on|off`)

Schema introspection uses:

- `PRAGMA table_info(table)` for columns and primary keys
- `PRAGMA foreign_key_list(table)` for FK graph ordering
- `sqlite_master` for SQL definitions of tables, views, indexes and triggers

By default, data copy is ordered by primary key (or `rowid`) and tables are ordered by foreign-key dependencies.

::: warning Tables without primary keys
Deterministic per-row values are derived from a row fingerprint instead of the key.
:::

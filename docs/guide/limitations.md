# Limitations

- SQLite only; no external SQL parser.
- Triggers and views are copied as-is and may have side effects during import.
- For tables without primary keys, deterministic per-row values are derived from a row fingerprint.
- Subsetting expands selections via foreign keys; complex custom join logic is not supported.
- Built-in faker coverage is intentionally small; use [plugins](/plugins/) for large catalogs or specialized generators.

# Subsetting

`pinkmask sample` seeds a selection from one or more root tables and expands it through foreign keys so the output stays referentially consistent.

```yaml
subset:
  roots:
    - table: users
      where: "country = 'US'"
      limit: 50
```

| Field | Meaning |
| --- | --- |
| `subset.roots` | List of roots to seed graph-aware subsetting |
| `subset.roots[].table` | Root table name |
| `subset.roots[].where` | SQL WHERE clause for root selection |
| `subset.roots[].limit` | Limit on root selection |

Per-table `where` and `limit` under `tables.<table>` are also honored by `sample`.

Complex custom join logic is not supported; expansion follows declared foreign keys only.

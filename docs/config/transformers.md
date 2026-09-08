# Transformers

All built-in transformers are deterministic given the same `--salt` and `--seed`.

| Type | Behaviour | Options |
| --- | --- | --- |
| `HashSha256` | Salted SHA-256 of the value | `maxlen` |
| `HmacSha256` | HMAC-SHA-256, salt as key | `maxlen` |
| `StableTokenize` | Short base32 token | `maxlen` |
| `RegexReplace` | Regex substitution | `pattern`, `replace` |
| `SetNull` | Replace with NULL | — |
| `SetValue` | Replace with a constant | `value` |
| `FakerName` | Deterministic fake name | — |
| `FakerEmail` | Deterministic fake email | — |
| `FakerAddress` | Deterministic fake address | — |
| `FakerPhone` | Deterministic fake phone | — |
| `DateShift` | Shift a date by a stable offset | `params.max_days` |
| `Map` | Dictionary lookup | `map` inline, or `lookup_table` / `lookup_key` / `lookup_value` |

Need more? Register your own via a [plugin](/plugins/).

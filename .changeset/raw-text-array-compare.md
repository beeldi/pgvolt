---
'@core/sync-service': patch
---

A where clause comparing arrays of `json`, `jsonb` or `bytea` with `=`, `<>`, `@>`, `<@` or `&&` is now refused when the shape is created. Electric compared their elements as the raw text Postgres sent, so a literal in another spelling, such as upper-case hex or keys in another order, never matched rows that Postgres matched.

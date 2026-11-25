---
'@core/sync-service': patch
---

`COLLATE` is matched on the whole qualified name: `"C"`, `"POSIX"` and their `pg_catalog` forms are accepted, and a collation named `C` in another schema is refused like any other collation.

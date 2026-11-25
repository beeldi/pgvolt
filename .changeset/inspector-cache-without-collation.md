---
'@core/sync-service': patch
---

A restart onto this version keeps its shapes: the schema check no longer reads a table cache persisted before the column info carried the collation as a schema change, and rewrites that cache instead.

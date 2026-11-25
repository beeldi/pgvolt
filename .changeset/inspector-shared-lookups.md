---
'@core/sync-service': patch
---

Let requests queued behind an inspector lookup that found no table, or hit a database error, share that answer instead of each repeating the lookup.

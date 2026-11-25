---
'@core/sync-service': patch
---

Stop sending a table's changes to a partitioned root it no longer belongs to, after the partition is detached, or dropped and recreated as a plain table under the same name.

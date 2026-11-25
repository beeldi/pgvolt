---
'@core/sync-service': patch
---

Write replicated booleans as `true`/`false`, as snapshots do, instead of `t`/`f`. A boolean in a primary key gave a row one key in the snapshot and another in later changes, so those changes never reached the row. Shapes that already exist on such a table should be recreated after upgrading.

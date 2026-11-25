---
'@core/sync-service': patch
---

A subquery move-in whose query finds no rows now ends with a `snapshot-end` like any other, so a client waiting for the move-in it saw announced learns it is complete.

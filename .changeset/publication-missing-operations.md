---
'@core/sync-service': patch
---

Restore the operations an existing publication stopped publishing, and recreate the replication slot so all shapes are invalidated, instead of restarting the connection forever.

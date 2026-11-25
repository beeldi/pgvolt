---
'@core/sync-service': patch
---

When the shape subsystem gives up after repeated crashes, the stack now shuts down so it restarts cleanly, instead of running on without it while the replication slot's WAL grows.

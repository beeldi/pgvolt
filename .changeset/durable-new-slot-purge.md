---
'@core/sync-service': patch
---

Discard shapes from before a recreated replication slot even when the service restarts before it can write the timeline purge marker.
Restarting with an existing slot leaves its shapes intact when slot creation was never attempted.
Instances waiting for the replication lock can still serve stored shapes read-only while slot creation is provisional.

---
"@core/sync-service": patch
---

Load completed shapes for read-only serving while waiting for the replication lock. Shapes completed after that load wait for the lock, like new ones. Validate the catalog and reclaim incomplete snapshots and orphaned shape storage after taking the lock.

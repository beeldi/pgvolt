---
"@core/sync-service": patch
---

A read-only request that finds lost shape data, such as one served by an instance waiting for the replication lock, now reports the loss without removing the shape, in an SSE stream's later segments too. A live request that started read-only keeps to the disk until the instance writes the shape, also while its status monitor restarts, asking a client ahead of the disk to retry rather than refetch, and reads through the writer as soon as the shape's consumer runs there. Removal is left to the instance that writes the shape; before, the waiting instance deleted it from the shared catalog and storage.

---
"@core/sync-service": patch
---

With `ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE`, a second instance sharing the shape catalog no longer makes the running one drop catalog writes or stop creating shapes. While the second instance's boot integrity check holds a read lock on the file, the running instance keeps its writes queued and retries them, for up to 20 seconds in its last flush at shutdown, instead of dropping each after 100 failed flushes and reopening its connection. Validation after the replication lock, and the refresh after it, read the catalog as those queued writes will leave it, where validation removed a shape whose completion was queued, its count took out twice a shape whose removal was queued, and the refresh put such a shape back; validation gives up 60 seconds after it starts.

---
"@core/sync-service": patch
---

Keep the shape catalog when its boot integrity check finds the file locked by another connection, such as a second instance sharing the storage, whether the lock is there when the check starts or arrives while it runs. The open fails and is retried on restart; before, the catalog was deleted as corrupt. A catalog query that meets another connection's lock now returns SQLite's busy error instead of raising, and no check reads that error as an answer: a waiting instance keeps serving the shapes it loaded, and a snapshot-complete mark that fails stays queued.

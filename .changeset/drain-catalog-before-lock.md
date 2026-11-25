---
"@core/sync-service": patch
---

Before it lets the replication lock go, on a stop or an idle disconnect, an instance tries to commit the shape catalog writes it still has queued, retrying through another process's lock on the catalog for up to 20 s. This is best effort: another instance that takes the lock next keeps a shape whose snapshot completed only if its mark has committed when it validates the catalog, and a write error, the end of those 20 s, or a session that ends first can leave the mark queued.

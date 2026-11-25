---
'@core/sync-service': patch
---

While the stack is read-only, a client ahead of the shape log on disk gets a 503 with retry-after instead of a 409 must-refetch. Read-only, the server sees only bytes synced to disk, and a client can be ahead of them after a stop that killed the writer; once active, the replay writes those changes again at the same offsets and the same request is answered. A request whose wait ends before the replay reaches its shape still gets the 409, as before.

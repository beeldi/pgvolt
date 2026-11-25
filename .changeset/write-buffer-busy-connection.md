---
'@core/sync-service': patch
---

A busy ShapeDb connection no longer takes the stack down or strands half-removed shapes. In exclusive mode readers share the one SQLite connection, so a storm of shape requests or a bulk shape cleanup could hold it past its 10 s checkout deadline. The write buffer then died and took the whole stack with it, and every shape reloaded cold; it now keeps the batch queued for the next poll without spending a flush attempt. The background shape cleanup tasks (the relation cleanup after a schema change, and the removal after a consumer ends) died the same way and left the shapes they had not reached in place; they now ask for the connection again until they get it.

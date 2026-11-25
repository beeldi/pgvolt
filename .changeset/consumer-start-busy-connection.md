---
'@core/sync-service': patch
---

Starting a consumer for a stored shape no longer restarts the stack when the ShapeDb connection is busy. In exclusive mode readers share the one SQLite connection, and right after a boot restore it can stay busy past the 10 s checkout deadline. The read that loads the shape for a transaction then exited and took the ShapeCache, and the whole stack, down with it. The caller now asks again until the connection frees up, for at most the log collector's stall timeout (10 minutes by default); past it the caller gets the error and the stack restarts as before.

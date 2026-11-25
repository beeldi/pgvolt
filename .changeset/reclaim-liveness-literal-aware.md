---
'@core/sync-service': patch
---

Generation reclaim no longer treats a superseded shape as live because one of its where clause literals contains the current rotation token. The liveness screen now looks for the `'token' = 'token'` comparison outside string literals, so a client-chosen value can no longer block the reclaim of its whole page.

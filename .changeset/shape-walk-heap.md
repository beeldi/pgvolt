---
'@core/sync-service': patch
---

A walk of the stored shapes gives its caller's heap room for a few hundred decoded shapes, instead of collecting garbage once every other shape. On a large bench store, restoring the filters takes about half as long, and each rebuild of the routing state 40% less.

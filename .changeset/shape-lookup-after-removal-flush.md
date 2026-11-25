---
'@core/sync-service': patch
---

A shape lookup no longer returns a shape whose removal was committed while the lookup was reading the shape catalog. Right after a restart, which drops every subquery shape, a client could be handed one of those dropped shapes and get a 409 on its first request.

---
'@core/sync-service': patch
---

A request that waits for a new shape's snapshot no longer times out after 45s when it reaches the shape's consumer before the consumer has finished starting.

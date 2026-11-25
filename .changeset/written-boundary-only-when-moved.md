---
'@core/sync-service': patch
---

A commit whose bytes are still in the writer's buffer no longer rewrites the shape's written-boundary file, which had put a file rename on every transaction of every shape.

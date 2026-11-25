---
'@core/sync-service': patch
---

A boot reads the stored shapes once: the routing state ShapeStatus keeps for every shape is built from the collector's restore instead of a read of its own in `ShapeStatus.initialize/1`. The shape totals metric is emitted once that restore has counted the shapes.

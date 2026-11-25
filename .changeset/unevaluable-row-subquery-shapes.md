---
'@core/sync-service': patch
---

`electric.shape.unevaluable_row_excluded.count` now also counts rows of a shape with subquery dependencies that are kept out because one of their subquery positions could not be evaluated. Before, only shapes without subqueries emitted it.

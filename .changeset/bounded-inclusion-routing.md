---
'@core/sync-service': patch
---

Bound inclusion-index work for long row arrays while preserving exact routing. Array containment
ignores NULL elements in a row array, as Postgres does.

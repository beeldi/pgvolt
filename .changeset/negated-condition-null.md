---
'@core/sync-service': patch
---

A snapshot row whose negated subquery condition is unknown (`NULL NOT IN (...)`) now reports that condition inactive, as live changes do, so the row leaves the shape when the condition that did include it stops holding.

---
'@core/sync-service': patch
---

The collector's restore now reads stored shapes in a single streaming pass instead of up to four passes. Uncastable filters and subquery-dependent shapes are identified during the routing rebuild pass, and the journal is checked for missing transaction IDs before reading shapes.

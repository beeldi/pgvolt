---
'@core/sync-service': patch
---

A boot reads each shape's digest and snapshot flag from the indexes that already hold them, and finds the snapshots that never completed the same way, instead of reading every stored shape. On a large bench store the two validations become near-instant, and the two metadata walks are about thirteen times faster.

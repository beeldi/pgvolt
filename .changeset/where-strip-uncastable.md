---
'@core/sync-service': patch
---

Removing a shape whose where clause filters on a column Electric cannot cast no longer crashes the shape log collector. Only a shape stored before such filters were refused can have one. The routing filter now keeps that clause as it was evaluated, instead of rebuilding it on removal with a parse that refuses it.

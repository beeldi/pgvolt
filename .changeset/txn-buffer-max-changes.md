---
'@core/sync-service': patch
---

A subquery shape, or one of its inner shapes, that holds more than 250,000 changes of one transaction in memory is now removed, so its clients refetch as after a truncate, and the other shapes keep going. Set `ELECTRIC_TXN_BUFFER_MAX_CHANGES` to change the limit.

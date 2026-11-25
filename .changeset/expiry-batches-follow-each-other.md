---
'@core/sync-service': patch
---

With `ELECTRIC_MAX_SHAPES` set, a store that is still over its limit after an expiry batch that removed shapes gets the next batch straight away instead of after the next period, even when as many shapes were created while the batch ran. Before, the number of shapes removed was capped at one batch per period, so shapes created faster than that were never brought back to the limit.

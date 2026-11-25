---
'@core/sync-service': patch
---

The relation tracker restores the publication filters from the shape metadata held in memory instead of reading every stored shape, so the collector's restore no longer waits for that read at boot.

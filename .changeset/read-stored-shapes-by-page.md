---
'@core/sync-service': patch
---

The shape log collector's boot restore reads the stored shapes a page of 100 per call into SQLite instead of a row per call. Each call runs on a dirty scheduler, and back from it the collector waited for a turn on schedulers the restore's processes keep busy, which made the read the bound of the restore. The rows, their order and the state the boot builds are the same. On a bench store of 100k shapes the read alone goes from 2.35 to 2.74 s down to 0.68 to 0.98 s with the schedulers busy, and the restore from a median of 3.3 s to 2.2 s.

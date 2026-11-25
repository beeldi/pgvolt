---
'@core/electric-telemetry': patch
'@core/sync-service': patch
---

The storage directory walk behind the storage-used metric now waits at least ten times as long as the last walk took, and its period is set with `ELECTRIC_DISK_USAGE_SCAN_PERIOD` (`0` turns it off). On a large shape store it used to start the next full walk one minute after the previous one ended.

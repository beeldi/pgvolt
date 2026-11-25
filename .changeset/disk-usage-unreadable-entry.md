---
'@core/electric-telemetry': patch
---

The storage disk-usage metric no longer drops to a fraction of the real size when a file is removed while the directory is being measured: an entry that cannot be read now adds nothing instead of resetting the total counted so far.

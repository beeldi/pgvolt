---
'@core/sync-service': patch
---

A shape request keeps an `existing` admission permit only while its handle names the requested shape at an offset that reads that shape. A request at offset `-1` or `now`, or whose handle belongs to another shape definition, counts against the `initial` limit.

---
"@core/sync-service": patch
---

A timeline or database change now records a durable purge obligation before purging shapes, so a restart or storage failure mid-purge resumes the purge on startup instead of serving stale shapes. While a purge is owed or the timeline record is unreadable, the service refuses to serve old shapes (even read-only), restarts the shape subsystem cleanly from the replication slot position, and withholds covered-LSN answers until the new collector catches up. With `ELECTRIC_KEEP_SHAPES_ON_PROMOTION` enabled, keepalive positions dropped during replay are still counted against the switchpoint.

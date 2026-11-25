---
"@core/sync-service": patch
---

Existing shapes remain available read-only while the stack waits for the Postgres advisory lock. For journal-backed shapes, read-only requests report the shape's persisted watermark, and after lock acquisition the stack waits for any outstanding boot-time replay before serving from the local writer. A request admitted before that, which the replay wakes, reports the shape's own offset as its watermark until the replay ends.

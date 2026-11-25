---
"@core/sync-service": patch
---

With file storage, a subquery shape that starts while the shape it depends on is still writing its snapshot no longer misses that shape's changes logged before the start. Its startup replay waited at the snapshot's end and got nothing back once the end was recorded, then took the log as read; that read now streams the log.

---
'@core/sync-service': patch
---

A shape's first synced commit and reopen now sync every directory along the path to its log and metadata files, and write log generation names durably. After a machine restart, reopening resumes from the last synced offset instead of keeping potentially zeroed or torn unsynced bytes, while same-boot reopens recover safely from short chunk indexes, compacted tails, or file errors without cutting short in-flight readers. Stale flush reports from a replaced writer no longer shorten its replacement's boot stall grace.

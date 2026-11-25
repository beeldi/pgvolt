---
'@core/sync-service': patch
---

Compaction now merges keys in offset order (preserving updates written since the previous compaction), cuts only at synced chunks past the previous boundary, publishes a new log generation only after copying and syncing it in full, and schedules a fresh flush timer for subsequent commits. Reopening a shape completes an interrupted compaction switch, cleans up orphaned generations, and rebuilds shapes whose compacted files, chunk index, or boundary are inconsistent or undecodable instead of serving short logs. Compactions also reject stale generation switches and clear their in-progress latch cleanly on failure.

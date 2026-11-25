---
'@core/sync-service': patch
---

With `ELECTRIC_ACK_AFTER_JOURNAL=true`, a crash that leaves the newest journal generation ending in a partial frame no longer stops replication for good. The next boot replayed it, but the first journaled event then opened a fresh generation, and from then on every read took the torn one for corruption: each reconnect failed in connection setup and no change reached the shapes until the journal was repaired by hand. The writer now cuts the newest generation back to its last whole frame, and syncs that to disk, before it opens the fresh one. The dropped bytes were never synced, so the slot was never acked past them and PostgreSQL sends them again.

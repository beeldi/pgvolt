---
"@core/sync-service": patch
---

Shape validation after the replication lock counts the catalog inside its own write transaction: its rows plus the additions the write buffer still holds. It no longer takes off a queued removal, which can still fail and leave the row, nor reads the buffer's counters, which take off a committed removal only after the commit. Either could make the count one short: 0 with a shape left, which on the first refresh deleted the storage of every shape, or -1 for an empty catalog, which skipped that cleanup.

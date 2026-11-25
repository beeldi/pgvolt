---
'@core/sync-service': patch
---

When a table is out of the publication, Electric now removes the shapes on it from before, and writes that removal to disk, before the table is served again, whether Electric adds it back, another session does, or the operator does in manual publishing mode: what was written to the table while it was out never reached them, so their clients refetch them. A shape that has not started its first snapshot on that table is kept, including one told to retry, and a shape that comes in meanwhile waits. If a removal is not on disk within three seconds of its start, that table is held, across a restart of the publication configurator too, and the shapes waiting for it retry, while the other tables go in.

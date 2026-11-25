---
'@core/sync-service': patch
---

A shape waiting for its snapshot that buffers more changes than `ELECTRIC_TXN_BUFFER_MAX_CHANGES` (250,000 by default) is now removed, so its clients refetch, instead of growing memory without bound when a bulk transaction lands during a slow snapshot.

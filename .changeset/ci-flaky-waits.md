---
'@core/sync-service': patch
---

Three tests no longer fail on a loaded runner: the publication manager tests retry an admission held for a removal not yet confirmed on disk, as a consumer does, the restored-shape admission test waits past the snapshotter's own timeout, and the test database setup gives `CREATE DATABASE` and `DROP DATABASE` 45 s.

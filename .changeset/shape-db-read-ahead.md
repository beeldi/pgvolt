---
'@core/sync-service': patch
---

The shape store file is read through sequentially before its integrity check, so a restart on a cold page cache no longer pays one 4 KB read per page. On a large bench store and a disk that serves few reads per second, opening the store is more than ten times faster.

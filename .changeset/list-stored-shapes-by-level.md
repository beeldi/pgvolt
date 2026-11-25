---
'@core/sync-service': patch
---

The file storage lists its stored shape handles one directory level at a time with `File.ls` instead of `Path.wildcard/1`, which built every path as a charlist and sorted them all. The listing gives the same set, and the orphaned shape storage reclaim at boot waits on it: on a large bench store it is about ten times faster.

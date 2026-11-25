---
'@core/sync-service': patch
---

A compaction covers only chunks at or below the shape's last synced offset, and syncs each log generation it writes, with the directory entries naming it, before it durably writes the name that points at it. It takes a file name past every generation on disk, never replaces an existing file, and its cleanup keeps the generation it published even when compactions left running by killed writers finished later. A writer reopened after a kill during a compaction's switch appends to the generation a restart reads, and a reopen that finds a file of its compacted generation missing rebuilds the shape instead of serving the offsets below the compaction empty.

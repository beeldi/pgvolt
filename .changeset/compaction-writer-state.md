---
'@core/sync-service': patch
---

A shape requested with `experimental_compaction=true` no longer loses rows after a compaction. The writer went on from a blank state and readers got the new log with the old one's positions, so reads missed or failed on the rows the compaction kept and on those written next, and a restart could drop rows the shape had acknowledged or rebuild it.

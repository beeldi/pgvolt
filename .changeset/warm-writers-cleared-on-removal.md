---
'@core/sync-service': patch
---

The log collector no longer keeps a record for every shape that ever flushed, and the record names the writer process that flushed. A removed shape is dropped from it, a late flush notification for an untracked shape is not recorded, and a new writer for a shape, after a crash or an idle suspension, gets the first-flush allowance again. Before, the record grew with each shape created and removed, and a new writer could be invalidated as stalled during a reconnect.

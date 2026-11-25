---
'@core/sync-service': patch
---

A transaction over 250 MiB no longer drops the replication slot and purges every shape. The size cap is now off by default, since transactions already reach the shapes in small fragments; `ELECTRIC_EXPERIMENTAL_MAX_TXN_SIZE` still sets one explicitly.

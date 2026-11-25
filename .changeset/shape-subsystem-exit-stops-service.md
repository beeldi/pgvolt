---
'@core/sync-service': patch
---

A process of the shape subsystem that exits now stops the service instead of the subsystem starting again in place, and the next start reads the replication slot again. A collector that exited in the middle of a transaction, or while it held a shape whose consumer had exited, could leave that shape up to date without a change Postgres had committed. Run the service under something that restarts it; an application that embeds the service restarts its stack itself.

---
'@core/sync-service': patch
---

After a restart, a shape's log keeps a transaction that reached the file after the last datasync, and syncs it before acknowledging it. Before, recovery trimmed the log back to the older durable offset whenever one existed, deleting committed transactions that readers had already been served. A log that a machine crash left short of such a transaction goes back to the durable offset instead of being rebuilt.

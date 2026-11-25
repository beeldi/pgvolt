---
'@core/sync-service': patch
---

With `ELECTRIC_ACK_AFTER_JOURNAL=true`, a replication connection that drops while transactions are journaled but not yet processed by the shape log collector no longer loses them. The slot was acked past those transactions, so PostgreSQL resumes after them, and only the collector's boot replay reads them back from the journal. On reconnect, when the journal holds a committed transaction past the last one the running collector processed, the shape subsystem now restarts on its existing storage, as after a crash, and replays it before streaming resumes. A reconnect with nothing pending keeps the shape subsystem running, as before.

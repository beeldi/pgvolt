---
"@core/sync-service": patch
---

Concurrent connection-restart requests are deduplicated by target connection manager so piled-up stops no longer trip the supervisor restart limit, and a publication check that races a connection restart returns a retryable error instead of dropping waiting shapes. Before an idle disconnect, the replication client keeps answering Postgres keepalives while draining queued shape-catalog writes and stays connected if replication traffic arrives.

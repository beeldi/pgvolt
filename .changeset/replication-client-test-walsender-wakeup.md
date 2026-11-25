---
"@core/sync-service": patch
---

Replication client tests that wait for streamed data now prompt the walsender every 500ms. On Postgres 16 and later a walsender can miss the wakeup for a commit and leave it unsent until the client's 15s keepalive, which made those tests fail intermittently on CI. The handler restart test also waits for the crashed handler to exit before replacing it, the scheduler priority tests restore `ingest_priority_high` instead of deleting it, and the silent-publication flush test waits for the slot to advance instead of running a single query.

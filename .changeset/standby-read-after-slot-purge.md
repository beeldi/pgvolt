---
'@core/sync-service': patch
---

An instance waiting for the replication lock reads the shared record once a second, and stops admitting requests for its stored shapes when it finds a purge the lock holder confirmed after recreating the slot. A provisional slot creation still allows read-only serving.

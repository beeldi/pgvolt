---
'@core/sync-service': patch
---

Add opt-in full transaction IDs and database generation metadata so clients can safely order long-lived snapshots and membership changes across transaction ID wraparound. Preserve the legacy shape protocol and correct acknowledgement handling at adjacent WAL transaction boundaries.

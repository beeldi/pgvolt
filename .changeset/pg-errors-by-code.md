---
'@core/sync-service': patch
---

Recognise the expected Postgres errors during connection setup by their SQLSTATE code, so Electric starts against a server whose `lc_messages` is not English.

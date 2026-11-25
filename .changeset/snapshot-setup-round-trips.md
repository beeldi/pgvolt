---
'@core/sync-service': patch
---

A shape snapshot now applies its display settings in the same query that reads the Postgres snapshot, so starting one takes two round trips to Postgres before the data query instead of eight.

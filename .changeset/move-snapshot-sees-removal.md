---
'@core/sync-service': patch
---

A subquery shape no longer sends, then deletes, a row a dependency removal had already taken out when a move's query read Postgres: when the move's snapshot sees every transaction of a queued removal, its rows stay out of the move's snapshot, as those of the move's own transaction did.

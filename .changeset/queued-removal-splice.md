---
'@core/sync-service': patch
---

A subquery shape no longer sends a row its WHERE clause never selected when a dependency loses a value while a move of one of its subqueries still runs its query: the changes buffered during the move, and the rows its query returns, now read that removal. The shape keeps its handle.

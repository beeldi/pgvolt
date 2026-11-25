---
'@core/sync-service': patch
---

`greatest()` and `least()` over intervals order them by their span, as Postgres does, so `greatest(interval '29 days', interval '1 month')` is the month on the live path as in the snapshot.

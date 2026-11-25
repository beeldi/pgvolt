---
'@core/sync-service': patch
---

`ELECTRIC_KEEP_SHAPES_ON_PROMOTION=true` keeps shapes when PostgreSQL moves to a new timeline by promotion rather than by rewind, as after a failover to a replica. Electric reads the new timeline's history and keeps shapes only when the stored timeline is an ancestor and everything Electric took from the stream is before the switch point; any other case purges as before and logs why. Full transaction provenance keeps its source generation across such a promotion. Off by default, and it needs `ELECTRIC_ACK_AFTER_JOURNAL=true`.

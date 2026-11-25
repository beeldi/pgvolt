---
'@core/sync-service': patch
---

At lock acquisition, replication no longer waits for the reclaim of orphaned shape storage, which lists every shape directory. The shape metadata refresh answers first and the reclaim runs right after it. With a large shape count on a slow disk that listing can take a long time at every boot. Requests with `source_positions=full_xid` get a 503 until replication delivers its first message, so they got that 503 for that long after the instance was already serving stored shapes read-only.

---
"@core/sync-service": patch
---

A shape's consumer no longer suspends while its snapshot is still being written, and suspends once the snapshot is done. A change that arrived in between restored the consumer on a snapshot whose end was not recorded yet, which the restore could take for an incomplete snapshot. A shape whose consumer suspends on a transient snapshot failure, as a restored shape with a complete snapshot does, is also no longer removed when the next request restores the consumer before the failed snapshot's process has exited: that process now gives up its name before it reports the failure.

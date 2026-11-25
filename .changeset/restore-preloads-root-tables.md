---
'@core/sync-service': patch
---

The boot restore's processes have the inspector introspect each root table they claim while the store is still being read, so restoring the partitions afterwards answers from the inspector's cache. On a large bench store, the partitions step after the read no longer takes any time.

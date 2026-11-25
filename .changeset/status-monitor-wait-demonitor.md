---
'@core/sync-service': patch
---

A caller that waits on the status monitor drops its process monitor once the wait succeeds. Before, each successful wait left one more monitor on the long-lived connection restarter or replication client, and a stale down message when the status monitor later exited.

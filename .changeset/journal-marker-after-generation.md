---
'@core/sync-service': patch
---

The transaction journal's used-marker is made durable only after the first generation exists. Before, a crash between the two left a marker beside an empty journal, and every later boot raised while replaying it until an operator removed the marker.

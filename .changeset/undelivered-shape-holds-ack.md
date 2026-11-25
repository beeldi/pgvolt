---
'@core/sync-service': patch
---

A shape whose consumer exits while a transaction is being delivered to it no longer lets the replication flush boundary pass that transaction at once. A crash exit invalidates the shape synchronously before the boundary moves, and an exit that may be a supervisor teardown (`:shutdown`, `:killed`, `:noproc`) pins the shape's flush entry, tracked or not, until the shape flushes past that transaction or the stall check resolves it, so a teardown still removes no shapes.

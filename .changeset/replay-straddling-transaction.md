---
'@core/sync-service': patch
---

A shape no longer crashes, and is no longer removed, when a restart replays a large transaction whose last change for that shape came in an early fragment. The shape's restored offset is its own last change in the transaction, not the transaction's end, so the replay's later fragments ended above it and reached the consumer with no transaction open. The consumer now skips every fragment of the transaction its offset belongs to. Each shape removed this way made its clients refetch it.

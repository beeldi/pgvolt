---
"@core/sync-service": patch
---

With `ELECTRIC_SHARED_PAYLOAD_ARENA` enabled, boot-time arena reclaim now aborts and prunes only empty generations whenever it cannot inspect the store root, a shard directory, or a listed shape's log directory (including broken symlinks or concurrent removals). Previously, unreadable shape directories were treated as referencing no payload, risking premature deletion of live arena generations.

---
"@core/sync-service": patch
---

With `ELECTRIC_KEEP_SHAPES_ON_PROMOTION` on, the promotion check now purges the shapes when the store's root or the journal's base path lists without the journal's entries while it reads them, as the directory a remount leaves under its mount point in their place does; it used to read that directory as a store with no shape or a journal that never wrote, and could keep shapes the old primary had streamed past the switch point. With `ELECTRIC_SHARED_PAYLOAD_ARENA` on, the boot's arena reclaim stops in the same case, pruning only the empty generations, when the store's root lists without the arena's directory, where it used to delete generations the shapes still read from.

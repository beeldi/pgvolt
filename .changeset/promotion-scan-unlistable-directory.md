---
"@core/sync-service": patch
---

With `ELECTRIC_KEEP_SHAPES_ON_PROMOTION` enabled, the promotion check now purges shapes whenever it cannot read every persisted position across the store root, shard directories, shape markers, or transaction journal (including broken symlinks or missing journal generations). It inspects only valid handle-mapped paths and ignores shapes moved out concurrently by orphan cleanup, preventing both unsafe retention and false purges.

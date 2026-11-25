---
'@core/sync-service': patch
---

Compaction no longer crashes on a key updated 65,535 times or more between two compactions. The entry merging those updates overflowed its 16-bit count, so the action file was read back misaligned and the compaction task died with `{:bad_generator, :eof}` on every run.

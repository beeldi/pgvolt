---
'@core/sync-service': patch
---

A boot reclaims payload-arena generations only once the generations left by earlier boots hold `ELECTRIC_PAYLOAD_ARENA_RECLAIM_MIN_BYTES` (1 GiB by default, 0 reclaims on every boot). The reclaim scans the log directory of every stored shape before the stack can go active, which with a large shape count costs minutes of a restart even when few bytes are reclaimed. Under the threshold the boot prunes the empty generations and keeps the rest.

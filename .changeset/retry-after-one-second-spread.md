---
"@core/sync-service": patch
---

A `503` that asks for a one-second `retry-after` now sends 1 or 2 seconds at random, so clients turned away by the same event no longer all retry in the same second. Every base is now spread up to twice its value, where the top value was one second short before.

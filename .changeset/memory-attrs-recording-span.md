---
"@core/sync-service": patch
---

A shape request whose trace is not sampled no longer reads its process's memory and binaries for span attributes that are never exported.

---
'@core/sync-service': patch
---

`/v1/health?require=read_only` answers 200 as soon as the stack serves its existing shapes (read-only mode), instead of 202 until it is fully active. With a large shape count that is minutes earlier in a boot, so a readiness probe using it lets clients resume their shapes while the restore finishes. Without the parameter, the endpoint answers as before, and the status text is unchanged either way.

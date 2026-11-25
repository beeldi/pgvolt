---
'@core/sync-service': patch
---

Two warnings no longer copy row data into the logs: a value that cannot be parsed is reported by its column and type only, and a failed shared conversion names the exception and where it was raised instead of quoting its message.

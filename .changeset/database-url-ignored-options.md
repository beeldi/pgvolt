---
'@core/sync-service': patch
---

A database URL query option other than `sslmode` is still ignored, but each ignored key is now named in a warning at startup, without its value. A misspelt `sslmod=require` no longer boots without a trace that `sslmode` was not set.

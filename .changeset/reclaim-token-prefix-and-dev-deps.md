---
'@core/sync-service': patch
'@electric-sql/client': patch
'@core/electric-telemetry': patch
---

Add an optional `token_prefix` parameter to `POST /v1/admin/shape-generations/reclaim`, rename its removal reason to `shape_generation_gc`, and refresh fork documentation, test hygiene, and development dependencies (`glob`, `minimatch`).

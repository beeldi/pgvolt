---
'@core/sync-service': patch
---

The shape database applies its journal mode each time it opens an existing file, not only when it creates one. A store created in WAL mode and reopened with `ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE=true` now switches to rollback mode, and the reverse.

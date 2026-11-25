---
'@core/sync-service': patch
---

The refresh that follows a boot's lock acquisition no longer reads every stored shape's metadata again when no other connection has committed to the shape store since the boot read it, which SQLite's `PRAGMA data_version` on the store's write connection tells. It logs "Shape metadata unchanged in the store since it was read, kept as read" instead. Any later refresh, or one after another instance wrote the store, reads it as before.

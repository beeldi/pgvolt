---
'@core/sync-service': patch
---

A locale-projected column now reads its generated `<column>_<locale>` column only if that column is in the request's `queryable_columns`, when the request sets them. Otherwise the column is served as stored, so a caller can no longer read an unlisted generated column by naming it as the locale.

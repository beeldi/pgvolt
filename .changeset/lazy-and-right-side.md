---
'@core/sync-service': patch
---

The filter's where clause analysis looks at the right side of an `AND` only when the left side gives no index. A clause such as `"firmId" = ? AND "divisionId" IN (...)` no longer analyses its `IN` list for nothing, which cuts the analysis from 916 to 214 reductions a shape on a bench store of 100k shapes. The filter it builds is the same.

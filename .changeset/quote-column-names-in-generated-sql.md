---
'@core/sync-service': patch
---

Column names holding a quote character are now escaped in generated SQL: subquery tags and regenerated where clauses no longer fail on such a column.

---
"@core/sync-service": patch
---

Shapes using `NOT IN` with nullable subquery keys now follow PostgreSQL membership rules as the dependency changes, with a refetch for ambiguous text-pair exclusions or overlapping dependency replacements that cannot be spliced safely. Shapes preserve explicit `C` and `POSIX` collations when generating SQL, admit scalar `greatest` and `least` alongside subqueries, and accept interval `ago` regardless of case or line breaks. Row-dependent text `coalesce`, `greatest`, and `least` in subquery shapes require explicit byte collation.

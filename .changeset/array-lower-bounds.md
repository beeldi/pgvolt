---
'@core/sync-service': patch
---

A row whose array has a lower bound other than 1, which Postgres writes as `[0:1]={a,b}` after an assignment such as `tags[0] = 'x'`, now reaches the shapes whose where clause selects it, where its live changes were dropped while the snapshot held it. Such an array compares as in Postgres: `=` and `<>` compare its bounds, subscripts count from them, and `@>`, `<@`, `&&` and `ANY` ignore them.

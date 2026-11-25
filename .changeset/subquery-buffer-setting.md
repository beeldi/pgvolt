---
'@core/sync-service': patch
---

Make `ELECTRIC_SUBQUERY_BUFFER_MAX_TRANSACTIONS` take effect. It was read into the application config but never passed to the stack, so subquery shapes always used the default of 1000 transactions buffered during a move-in, whatever the variable said.

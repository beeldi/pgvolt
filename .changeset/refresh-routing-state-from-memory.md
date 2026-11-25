---
'@core/sync-service': patch
---

The refresh that follows lock acquisition no longer reads every stored shape to recount which are indexed. It keeps what the boot worked out for the handles still stored, and reads the shapes only when another instance stored one in the meantime.

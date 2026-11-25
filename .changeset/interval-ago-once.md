---
'@core/sync-service': patch
---

An interval literal takes `ago` once, as its last word, as Postgres does: `'1 day ago ago'` is refused as invalid input instead of being read as one day.

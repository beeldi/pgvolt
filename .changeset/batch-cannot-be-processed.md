---
'@electric-sql/client': patch
---

Keep the offset when a batch cannot be processed, so the batch is asked for again and an SSE stream no longer skips it.

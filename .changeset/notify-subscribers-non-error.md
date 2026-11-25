---
'@electric-sql/client': patch
---

Tell subscribers when the stream stops after a failure that is not an `Error` (for example a custom `fetchClient` rejecting with a string or an object) by passing them an `Error` that keeps the original value as `cause`.

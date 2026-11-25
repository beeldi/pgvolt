---
'@electric-sql/client': patch
---

Retry with backoff when a shape response body is cut off mid-read, as is already done when the connection drops before the headers, instead of stopping the stream.

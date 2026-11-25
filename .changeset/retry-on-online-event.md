---
'@electric-sql/client': patch
---

End a stream's retry backoff early when the browser fires `online`, so a stream that lost its connection retries as soon as the network is back instead of waiting out its backoff timer.

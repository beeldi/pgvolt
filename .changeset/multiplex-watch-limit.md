---
'@core/sync-service': patch
---

A multiplex WebSocket connection accepts up to 100,000 active watches. A watch beyond that gets a retryable `too_many_watches` error frame.

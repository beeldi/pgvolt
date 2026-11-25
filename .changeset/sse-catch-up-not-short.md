---
'@electric-sql/client': patch
---

Stop counting an SSE response that delivered messages and moved the stream's
offset, or that ended inside a catch-up chunk, as a short connection. The server
closes a live SSE request that is behind the log end right after the catch-up,
so a burst of writes could make the client fall back to long polling for the
rest of the stream's life.

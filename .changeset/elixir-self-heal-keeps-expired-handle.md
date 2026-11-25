---
'@core/elixir-client': patch
---

After the stale-cache retries run out, a response that carries the known-expired shape handle is no longer accepted as fresh. The client stops sending the handle as a request parameter, keeps it to check the next response, and ends with the self-heal error when the same handle comes back. Clearing the handle from the expired-shape cache now leaves a newer handle that another poll recorded.

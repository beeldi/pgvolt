---
'@core/elixir-client': patch
---

Honour the `Retry-After` header on retried HTTP responses such as `503`. The client now waits at least as long as the server asks (delta-seconds or HTTP-date, at most one hour) and otherwise keeps its own jittered backoff, as the TypeScript client does.

---
'@core/elixir-client': patch
---

Retry a request that raced the start of its connection pool (`pool_not_available`) instead of failing it.

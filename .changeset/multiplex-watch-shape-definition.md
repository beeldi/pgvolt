---
"@core/sync-service": minor
---

A multiplex watch frame now carries the definition of the shape it watches
(`shape`, the query parameters of the HTTP request for that handle). Electric
arms the watch only when the handle is the shape of that definition and answers
`shape_mismatch` otherwise, so a caller holding someone else's handle can no
longer learn whether that shape changed. A watch without `shape` is refused.

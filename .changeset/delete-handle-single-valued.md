---
"@core/sync-service": patch
---

`DELETE /v1/shape` answers `400` when `handle` is given twice or spelled another way, as it already did for the parameters that define a shape.

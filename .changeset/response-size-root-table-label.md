---
'@core/sync-service': patch
---

The `root_table` label of `electric.shape.response_size.bytes` is now the root table of the shape the request resolved to, written as `schema.table`. Before, it was the raw `table` query parameter, which an API configured with a predefined shape does not validate.

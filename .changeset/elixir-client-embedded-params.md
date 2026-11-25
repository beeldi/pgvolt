---
'@core/elixir-client': patch
---

Fix parameterised `where` clauses (`$1`, `$2`, ...) with `params:` on a `ShapeDefinition` when the client runs in embedded mode. They failed with "parameter $1 was not provided" on `poll/4`, `stream/3` and `delete_shape/2`, and now work as they do over HTTP.

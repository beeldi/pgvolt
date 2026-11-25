---
'@core/elixir-client': patch
---

Fix decoding of escaped backslashes in quoted PostgreSQL array elements. `\\` now decodes to a single backslash, and an element that ends in a backslash no longer swallows its closing quote and the elements after it.

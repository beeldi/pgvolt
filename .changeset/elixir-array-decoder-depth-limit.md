---
'@core/elixir-client': patch
---

The array decoder refuses a PostgreSQL array literal nested deeper than the 6 dimensions PostgreSQL allows, with an `Ecto.CastError`, instead of recursing once per brace.

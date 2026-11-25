---
'@core/elixir-client': patch
---

A row value that cannot be cast no longer appears in the error the client logs. The log names the kind of failure, and for an Ecto schema the column and its type, instead of the formatted exception, which carried the raw value. The exception the client re-raises is unchanged.

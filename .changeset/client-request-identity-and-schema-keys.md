---
'@core/elixir-client': patch
---

Requests are coalesced only between clients with the same fetch options. Schema column names are now strings, as `Client.schema()` always documented, and column attributes the client does not know are dropped. A custom `ValueMapper` that matched atom column names must match strings.

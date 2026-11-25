---
"@core/sync-service": minor
---

Set the experimental `ELECTRIC_SUBSET_MAX_ROWS` option to cap the rows in a subset snapshot. Requests whose limit exceeds the cap or whose unbounded result would exceed it return `400`. A multiplex watch whose shape carries a subset, in any spelling, is refused with `invalid_shape`, since the shape endpoint answers a subset request at once with its rows.

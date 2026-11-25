---
'@core/sync-service': patch
---

A shape whose subquery selects a column holding a value Electric cannot parse, such as a float `NaN` or `Infinity`, a date or a timestamp at `infinity` or before the common era, a time of `24:00:00` or an interval at `infinity`, now serves the rows Postgres selects, in the snapshot, live and on a move in or out. It used to stop the shape. A subquery over a table whose deferrable primary key one transaction reuses now stops its view with a warning that names no key, instead of a crash report that logged it, and the shapes over it are invalidated as before.

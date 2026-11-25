---
'@core/sync-service': patch
---

A where clause comparing two arrays of float, time, timestamp or interval values (`=`, `<>`, `@>`, `<@`, `&&`, `= ANY`) now keeps the rows Postgres returns, where it missed a `-0` against `0`, an `ARRAY[0]::float8[]` constant, or another spelling of the same time or interval. A numeric `-0.00` in a where clause now equals zero, as in Postgres. A subquery keyed on a numeric now compares the key exactly: a `NaN` key moves rows in and out instead of making the shape refetch, and a key past float precision no longer matches its neighbour.

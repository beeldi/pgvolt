---
'@core/sync-service': patch
---

A subquery keyed on a single array column, such as `"divisionCodes" IN (SELECT codes FROM divisions)`, now moves rows in and out instead of making the shape refetch, and a `numeric[]` key now compares exactly. A row whose numeric key Postgres writes at another scale than the moved key (`1.00` for `1.0`) now leaves and enters the shape with it, where it stayed on the client after its key left, and a row inserted in the transaction that takes its key out of a subquery, or puts it in a negated one, no longer reaches the client, nor does a delete of a row the client never held. An `ARRAY[...]` argument with a NULL element, as in `1 = ANY(ARRAY[x, NULL])`, now keeps the rows Postgres returns.

---
'@core/sync-service': patch
---

A composite subquery whose select list does not name its columns sorted by name, byte by byte (uppercase first), such as `("divisionId", "firmId") IN (SELECT id, "firmId" FROM divisions)`, now pairs them in the order it names them, as Postgres does. It paired them sorted by name, so the shape held rows Postgres does not select and missed rows it does, or got a `400` when the paired columns had different types. Such a shape gets a new handle, and its clients refetch.

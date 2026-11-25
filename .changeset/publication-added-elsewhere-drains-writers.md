---
'@core/sync-service': patch
---

When another session adds a table to the publication between Electric's check and its own addition, the shapes waiting for that table now take their snapshot only once the writers then in flight have finished, as when Electric adds the table itself. A table Electric found out of the publication is served only once that wait, an addition or a replica identity change has gone through, so neither a timeout nor a restart of the publication configurator skips it.

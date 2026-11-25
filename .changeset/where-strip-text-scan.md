---
'@core/sync-service': patch
---

Adding a shape to the routing filter no longer parses its where clause to decide whether the evaluated clause can be dropped from memory: a read of the query text settles the plain clauses, and the parse remains for the others. A restore adds every stored shape, so it saves one parse per shape.

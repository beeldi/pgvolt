---
'@core/sync-service': patch
---

Deciding whether a where clause can use the routing index analyses each part of the clause once. A clause nesting `AND` over `OR` level after level took twice as long at each level, so a clause of a few hundred bytes could hold the creation of its shape for hours.

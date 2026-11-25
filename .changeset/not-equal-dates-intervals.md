---
'@core/sync-service': patch
---

A where clause can use `<>` (or `!=`) and `IS [NOT] DISTINCT FROM` between two dates, times, timestamps or intervals. Electric refused them with "Could not select an operator overload" when the shape was created, though the shapes guide lists them.

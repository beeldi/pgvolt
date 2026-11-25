---
'@core/sync-service': patch
---

An `AND` over an `IN` list or an `OR` is split into indexed branches only while the copies of the rest of the clause that split's branches hold stay within 16,384 nodes. Past that the shape is routed by evaluating its clause, as upstream routes it. The budget counts each split on its own, not the shape: a split inside another one's branches, or each term of a long `AND` chain that ends in an `IN` list, counts its copies afresh, so a shape can hold more than 16,384 nodes of copies.

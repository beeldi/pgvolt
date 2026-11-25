---
'@core/sync-service': patch
---

The shape log collector's boot restore analyses each stored shape's where clause once, for both its routing state and the filter, instead of once for each. The analysis was 30 % of a restored shape's work once decoded. The filter and the routing state it builds are the same.

---
"@core/sync-service": patch
---

The oracle property tests now fail a shape check whose client is not up to date when the check's timeout passes, instead of comparing the partial rows with the oracle.

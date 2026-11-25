---
'@core/sync-service': patch
---

Test only: the shape-walk heap test gives its 1,000-shape setup flush the same 120 s timeout as the other bulk shape-store tests, so a slow CI runner no longer fails it after 5 s.

---
'@electric-sql/client': patch
---

The empty-shape integration test now checks that SSE mode makes a single live request, instead of counting the catch-up requests before it, whose number depends on how far the snapshot had got.

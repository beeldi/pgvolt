---
'@electric-sql/client': patch
---

The live request timeout now counts the time since a live long-poll last received data, not the time since it started. A large response that keeps arriving is no longer aborted and retried forever, and a body that stops arriving is still aborted.

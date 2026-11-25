---
'@electric-sql/client': patch
---

The Postgres array parser refuses a literal nested deeper than the 6 dimensions Postgres allows, with an error, instead of recursing until the call stack is exhausted.

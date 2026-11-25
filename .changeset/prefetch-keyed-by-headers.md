---
'@electric-sql/client': patch
---

A chunk prefetched under one set of request headers is no longer used for a request with other headers, such as a new token. The client aborts the prefetches and fetches afresh.

---
'@core/sync-service': patch
---

In manual publishing mode or on publications owned by another role, Electric drains in-flight writers before admitting a newly published table and verifies publication membership under the lock regardless of default isolation level. Drain concurrency reserves an admin pool connection where possible (using exponential backoff from 100 ms up to 5 s on a single-connection pool), persists obligations across restarts, keeps waiting consumers and dependent subquery shapes alive while admission retries, and relies on operator-side locking when Electric lacks table lock privileges.

---
'@core/sync-service': patch
---

Test only: the oracle test's brutal restart now waits for every process of the killed stack to exit before starting the new one, which stops the new stack from adopting the old stack's tables and shutting down without an error log.

---
'@core/sync-service': patch
---

A long poll that parked while the stack was read-only now reads new changes that arrive once the stack is active through the shape's writer, as a request starting then would. It read the disk alone before, where only synced bytes count: it found nothing new, parked again, and took the disk's older offset for a regression, so the client got a 409 must-refetch and refetched the shape from scratch. While the stack is still read-only, it keeps reading the disk.

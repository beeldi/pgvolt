---
'@core/sync-service': patch
---

A retryable failure to register a restored shape with the publication, such as the connection subsystem restarting, suspends the shape instead of deleting it when its snapshot is already complete. The shape is still cleaned up when the failure is not retryable, when it comes after a snapshot began streaming, or when its consumer exits while still holding a transaction it has not flushed, for which the log collector removes the shape, at once or at its stall check.

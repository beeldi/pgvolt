---
"@core/sync-service": patch
---

Reopening a shape whose storage files are missing, unreadable, unsupported, or missing version/snapshot markers now removes the shape so clients refetch under a new handle instead of rebuilding under the old handle and missing intermediate changes. With file storage, a shape removed concurrently while a change restores its consumer is no longer recreated under its old handle, and a new shape whose snapshot fails before starting is removed cleanly while restored shapes retain their handle across retryable failures. Restoring a shape during a shape-table restart no longer drops the shape or crashes routing.

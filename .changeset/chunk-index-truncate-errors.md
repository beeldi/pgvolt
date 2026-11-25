---
"@core/sync-service": patch
---

A chunk-index cut that fails when a shape's writer reopens now raises the truncate's `File.Error`: a reopen checking the written boundary falls back to the durable one, and the shape fails to open when the durable recovery's own cut fails. Before, the failure went unnoticed, and unless a later cut took the stale bytes off, the writer could open its next chunk after a stale closing boundary, keep a stale opening as its current chunk, or fail the reopen on a `FunctionClauseError` over a partial record. A log trim whose seek fails now raises instead of cutting the log where no entry ends, and a trim of a log that ends before the chunk index's position no longer grows it with zeros.

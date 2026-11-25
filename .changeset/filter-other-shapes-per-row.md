---
'@core/sync-service': patch
---

The shapes a filter node cannot index are stored as one ETS row each, instead of one map per node that every add and remove copied out of ETS and back. Building the filter is no longer quadratic in the shapes sharing a node. Based on upstream's `fix/other-shapes-per-row-ets`, with the rows keyed on the node so that routing reads them in one lookup.

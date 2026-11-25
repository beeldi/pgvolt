---
'@core/sync-service': patch
---

The shape log collector's boot restore spreads the stored shapes over one process per scheduler, by root table, instead of decoding and routing every one of them itself. The collector reads the store and hands each row on still encoded; the processes decode the shapes, add them to the filter, whose tables they now write together, and send the rest back for the collector to merge. On a large bench store the step is more than three times faster, and the state the boot leaves in memory is the one the collector built alone.

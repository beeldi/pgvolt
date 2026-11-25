---
'@core/sync-service': patch
---

The shape store keeps each shape compressed (`term_to_binary`'s `compressed: 1`): 1.4 KB instead of 10.5 KB on a bench store's shapes, which makes the store's file about 4 times smaller once vacuumed, and the boot's integrity check as much faster. Shapes stored before are read as they are, and an earlier image reads the shapes stored now.

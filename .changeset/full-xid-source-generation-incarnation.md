---
'@core/sync-service': patch
---

Change `electric-source-generation` when the database or replication slot is recreated under the same name, by binding it to the database OID and a slot incarnation persisted in Electric's state. The generation changes once on upgrade, so full-provenance clients rebuild their membership cache once.

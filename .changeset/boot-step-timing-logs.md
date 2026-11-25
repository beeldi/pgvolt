---
'@core/sync-service': patch
---

Every boot step that reads the whole shape store or the whole shape storage tree now logs how long it took ("Boot step shape_db.integrity_check took 1234ms"), so a slow restart can be attributed from the log.

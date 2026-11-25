---
'@electric-sql/client': patch
---

A subset snapshot now only moves a stream to an earlier offset, so it cannot
skip changes to other rows. Concurrent snapshots on a cold stream resume from the
earliest of their offsets whichever returns first, and a snapshot that hits a 409
no longer leaves the stream on the new handle with the old handle's offset.

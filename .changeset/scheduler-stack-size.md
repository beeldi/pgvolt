---
'@core/sync-service': patch
---

The release gives each scheduler thread a 32 MB stack (`+sss 4096`), so that the SQL parser can read the most deeply nested where clause its 64 KB input limit admits. The stack is reserved address space; a thread only uses what its deepest parse needs.

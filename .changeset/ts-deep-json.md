---
'@electric-sql/client': patch
---

A `json` or `jsonb` column holding a document nested a few thousand levels deep no longer stops the stream, unless the document holds an integer past 2^53 and the client runs on Firefox. The parser revives integers past 2^53 only in documents that can hold one, and keeps them as numbers in a document too deep to revive.

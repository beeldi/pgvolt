---
'@core/sync-service': patch
---

`ELECTRIC_TWEAKS_MAX_REQUEST_LINE_LENGTH` sets the longest HTTP/1 request line Electric accepts, so shapes with a long `where` or many `params` no longer have to be split to stay under Bandit's 10,000-byte default.

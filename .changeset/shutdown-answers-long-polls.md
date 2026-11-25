---
'@core/sync-service': patch
---

A live request parked in a long poll or an SSE stream now ends at once when the server stops its connection, instead of waiting out the long poll: it answers as at the timeout, or with a 503 and retry-after when the client is ahead of this instance's log. ThousandIsland stops its acceptors one after another, each waiting for its connections, so under live traffic a stop took minutes (849 s on a bench with 200 clients long-polling, 1.1 s with this change).

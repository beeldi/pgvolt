---
"@core/sync-service": minor
---

Add an experimental option that records only the presence and size of query strings, predicates and subset parameters in traces, request logs and Sentry request events, and omits the free-form text of request errors. Logs and crash reports outside the request, snapshot and streaming paths keep their text.

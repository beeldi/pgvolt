---
'@core/sync-service': patch
---

`/v1/shape` and its subpaths (the multiplex socket included) accept the API secret in an `authorization: Bearer` header, which wins over the `secret` and `api_secret` query parameters, so a caller can keep the secret out of URLs that proxies and load balancers log.

A Sentry event built from a crash report no longer carries the request's `Plug.Conn` in its `logger_metadata`, whose headers and query string hold the secret.

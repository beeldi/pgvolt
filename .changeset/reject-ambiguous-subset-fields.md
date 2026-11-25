---
"@core/sync-service": patch
---

Shape POST requests return `400` when a subset field (`where`, `order_by`, `limit`, `offset` or `params`) appears in both the URL and the body, or in both the flat and nested body, and when the URL gives `subset` a value that is not an object next to subset fields in the body, which raised and returned `500`. Duplicate JSON member names in the body also return `400`, as does a key repeated in subset `params` given as a JSON string, in a GET query or a POST body.

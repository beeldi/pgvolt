---
'@core/sync-service': patch
---

Release a request's admission permit when its process is stopped from outside. A Bandit HTTP/2 stream process is killed with its connection and never ran its own release, so each connection closed during a live request left a permit counted until the server restarted, and enough of them turned every new request away with `503 concurrent_request_limit_exceeded`. Permits are now recorded against the request process, and AdmissionControl releases them when that process exits. Permits released this way are counted on the Prometheus endpoint as `electric_admission_control_reclaim_initial` and `electric_admission_control_reclaim_existing`.

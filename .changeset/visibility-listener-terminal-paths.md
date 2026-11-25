---
'@electric-sql/client': patch
---

A running stream that ends because the caller's signal aborts, its retries run out, an error is unrecoverable, or a `subscribe: false` stream completes now removes its visibility listener, so the stream is no longer kept alive by the page or the runtime adapter. A live stream that a hidden page paused still resumes.

---
"@core/sync-service": patch
---

A `tracestate` sample-rate hint larger than a signed 64-bit integer is ignored, like an unparseable one, instead of being stamped on spans as `SampleRate`.

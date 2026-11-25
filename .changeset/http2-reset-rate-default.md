---
'@core/sync-service': patch
---

Update Bandit and the other runtime dependencies that have published fixes. An HTTP/2 connection now carries at most 500 concurrent streams, Bandit's new default, and may send 10,000 stream resets in 10 seconds before it is closed. Set `ELECTRIC_TWEAKS_HTTP2_MAX_RESET_STREAM_RATE=disabled` for the previous behaviour.

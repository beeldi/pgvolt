---
"@core/sync-service": patch
---

Expose Bandit's HTTP/2 reset-stream rate limit as
`ELECTRIC_TWEAKS_HTTP2_MAX_RESET_STREAM_RATE` (default 10,000 resets in 10 seconds, or
`disabled`). Bandit's lower upstream limit closes an entire connection once a client sends more
than 500 `RST_STREAM` frames in 10 seconds, dropping every request in flight on it; behind a
multiplexing proxy, routine client-side cancellations of live requests can trip it and drop
unrelated long-polls.

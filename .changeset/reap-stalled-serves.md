---
'@core/sync-service': patch
---

Terminate shape responses whose clients stop accepting data. Such a response never completes and never shows up in request telemetry, yet it holds response memory and a file descriptor for as long as the connection lives. Response bodies are now written to the socket in pieces of at most 256 KiB, and a per-stack watchdog terminates the handler when one piece does not complete within `ELECTRIC_STALLED_SERVE_TIMEOUT` (default 60s; 0 turns it off). A live client only has to drain about one OS send buffer per window, and a terminated client resumes from its last offset. Each termination logs a warning with the shape handle and emits `[:electric, :plug, :serve_shape, :reaped]`, counted on the Prometheus endpoint as `electric_plug_serve_shape_reaped_count`.

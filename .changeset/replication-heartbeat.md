---
'@core/sync-service': patch
---

`ELECTRIC_REPLICATION_HEARTBEAT_INTERVAL` makes Electric write a logical decoding message through its admin pool at that interval and read it back on its own slot. Any message read counts as stream activity, so with `ELECTRIC_REPLICATION_IDLE_TIMEOUT` set, a quiet database no longer closes Electric's connections every idle window and makes live requests fail while it wakes; only a stream that stopped reading its socket still trips the timeout. Off by default; the interval must be at most half of the idle timeout.

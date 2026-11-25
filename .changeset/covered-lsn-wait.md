---
"@core/sync-service": minor
---

Add an opt-in `wait_for_lsn` shape parameter. Its up-to-date message carries `covered_lsn`, below
which every transaction that changes the shape is already in the response or before it, and a live
request with it also returns without a change once that LSN is covered.

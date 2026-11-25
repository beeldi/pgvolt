---
'@core/sync-service': patch
---

`POST /v1/admin/shape-generations/reclaim` removes, one page of the shape store at a time, the shapes whose where clause ends with a rotation token at or below `newest_collectable_generation`, and refuses the page when a selected shape names `current_generation` anywhere in its where clause or is a dependency another shape still reads. Admin routes take the API secret from the `authorization: Bearer` header only and answer 401 in insecure mode.

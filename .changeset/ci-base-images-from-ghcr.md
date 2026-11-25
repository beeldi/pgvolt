---
'@core/sync-service': patch
---

The Dockerfile takes a `BASE_IMAGE_REGISTRY` build argument, `docker.io` by default, so CI can build from its GHCR copies of the base images.

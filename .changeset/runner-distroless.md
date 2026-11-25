---
'@core/sync-service': patch
---

The image runs on distroless `cc-debian12` with only what the release scripts and the Erlang VM call: bash, coreutils, grep, sed, awk and the libraries they load. curl, apt, dpkg, perl and the locale tools are gone, so the image's `HEALTHCHECK` goes too, since it called curl; the dev compose checks `/v1/health` with bash instead.

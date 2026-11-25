ARG ELIXIR_VERSION=1.20.5
ARG OTP_VERSION=29.1.3
ARG DEBIAN_VERSION=bookworm-20261123-slim
ARG RUNNER_DEBIAN_VERSION=bookworm-20261104-slim

# CI points this at its GHCR copies of the two images below.
ARG BASE_IMAGE_REGISTRY=docker.io

ARG BUILDER_IMAGE="${BASE_IMAGE_REGISTRY}/hexpm/elixir:${ELIXIR_VERSION}-erlang-${OTP_VERSION}-debian-${DEBIAN_VERSION}@sha256:feeeefac76e848dd8cc8b003ebfadf00225db2a4bbe195a3f6442b8a807b308a"
ARG RUNNER_IMAGE="${BASE_IMAGE_REGISTRY}/debian:${RUNNER_DEBIAN_VERSION}@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc"
ARG DISTROLESS_IMAGE="gcr.io/distroless/cc-debian12@sha256:e5d81ddde149641e2a9ba55be4545bc125c67de07508b03ba4c22e6eb0ded5aa"
ARG MIX_TARGET=application

FROM ${BUILDER_IMAGE} AS builder
LABEL maintainer="info@electric-sql.com"

RUN apt-get update -y && \
  apt-get install -y build-essential git curl && \
  apt-get clean && \
  rm -f /var/lib/apt/lists/*_*

RUN mix local.hex --force && mix local.rebar --force

ARG MIX_ENV=prod
ARG MIX_TARGET

WORKDIR /builder/electric

COPY --from=electric-telemetry / /builder/electric-telemetry

COPY mix.* /builder/electric/
RUN mix deps.get
RUN MIX_OS_DEPS_COMPILE_PARTITION_COUNT=1 mix deps.compile

# These are ordered by change frequency, with the least frequently changing dir first.
COPY rel /builder/electric/rel
COPY lib /builder/electric/lib/

COPY package.json /builder/electric/
COPY config/config.exs /builder/electric/config/

# Set ELECTRIC_VERSION as late as possible to avoid invalidating the build cache for earlier steps
ARG ELECTRIC_VERSION

# The Electric app must be compiled after ELECTRIC_VERSION has been set
RUN mix compile
RUN mix sentry.package_source_code

COPY config/runtime.exs /builder/electric/config/

RUN mix release

RUN ls -l /builder/electric/_build

FROM ${RUNNER_IMAGE} AS runtime-packages

WORKDIR /packages
RUN apt-get update -y \
  && apt-get download libc6 libgcc-s1 libstdc++6 libssl3 libtinfo6 bash coreutils grep sed mawk libacl1 libattr1 libgmp10 libpcre2-8-0 libselinux1 libc-bin \
  && mkdir -p /runtime/var/lib/dpkg/status.d /runtime/usr/lib/locale \
  && dpkg-deb --fsys-tarfile libc-bin_*.deb | tar -x -C /runtime ./usr/lib/locale/C.utf8 \
  && rm libc-bin_*.deb \
  && for deb in *.deb; do \
       package="$(dpkg-deb --field "$deb" Package)"; \
       dpkg-deb --extract "$deb" /runtime; \
       dpkg-deb --field "$deb" > "/runtime/var/lib/dpkg/status.d/$package"; \
       dpkg-deb --ctrl-tarfile "$deb" | tar -xO ./md5sums > "/runtime/var/lib/dpkg/status.d/$package.md5sums"; \
     done \
  && ln -s bash /runtime/bin/sh \
  && ln -s mawk /runtime/usr/bin/awk \
  && rm -rf /runtime/etc/skel /runtime/usr/share/doc /runtime/usr/share/doc-base /runtime/usr/share/info /runtime/usr/share/lintian /runtime/usr/share/locale /runtime/usr/share/man /runtime/usr/share/menu

FROM ${DISTROLESS_IMAGE} AS runner

ARG MIX_TARGET
ARG RELEASE_NAME=electric

COPY --from=runtime-packages /runtime /

ENV LANG=C.UTF-8 \
  MIX_ENV=prod \
  MIX_TARGET=application \
  RELEASE_TMP=/tmp

WORKDIR /app

RUN chown nobody /app

COPY --from=builder "/builder/electric/_build/${MIX_TARGET}_prod/rel/${RELEASE_NAME}" ./

RUN mv /app/bin/${RELEASE_NAME} /app/bin/entrypoint

# 65534 is `nobody`, the owner /app is chowned to above. Numeric rather than the name so a
# Kubernetes `runAsNonRoot: true` can verify the identity without resolving it against /etc/passwd.
# The release only reads /app; it writes to ELECTRIC_STORAGE_DIR, so the mounted volume has to grant
# this identity write access (a pod `fsGroup`) or the app aborts at boot on mkdir.
USER 65534:65534

ENTRYPOINT ["/app/bin/entrypoint"]

CMD ["start"]

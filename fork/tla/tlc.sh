#!/usr/bin/env bash
set -euo pipefail

readonly TLA_TOOLS_VERSION=1.7.4
readonly TLA_TOOLS_SHA256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
readonly JAVA_IMAGE=eclipse-temurin:21-jre
readonly MEMORY=${TLC_MEMORY:-6g}
readonly HEAP=${TLC_HEAP:-3g}
readonly WORKING_FILES=${TLC_WORKING_FILES:-2g}
readonly WORKERS=3
readonly TIME_LIMIT=${TLC_TIME_LIMIT:-30m}

if [ "$#" -ne 2 ]; then
  echo "usage: tlc.sh <Model.tla> <run.cfg>" >&2
  exit 2
fi

model_dir=$(cd "$(dirname "$1")" && pwd)
model=$(basename "$1")
config_dir=$(cd "$(dirname "$2")" && pwd)
config=$(basename "$2")
run=${config%.cfg}
output="$config_dir/$run.out"

matches_release() {
  echo "$TLA_TOOLS_SHA256  $1" | shasum -a 256 -c --status
}

simulate=$(sed -n 's/^\\\* simulate: //p' "$2" | head -1)
mode=()
if [ -n "$simulate" ]; then
  read -r traces depth seed <<<"$simulate"
  mode=(-simulate "num=$traces" -depth "$depth" -seed "$seed")
fi

capture=$(mktemp)
download=""
trap 'rm -f "$capture" "$download"' EXIT

cache="${XDG_CACHE_HOME:-$HOME/.cache}/electric-model-check"
jar="$cache/tla2tools-$TLA_TOOLS_VERSION.jar"
if [ ! -f "$jar" ]; then
  mkdir -p "$cache"
  download=$(mktemp "$cache/download.XXXXXX")
  curl -fsSL -o "$download" "https://github.com/tlaplus/tlaplus/releases/download/v$TLA_TOOLS_VERSION/tla2tools.jar"
  if ! matches_release "$download"; then
    echo "the downloaded tla2tools.jar does not match the sha256 of v$TLA_TOOLS_VERSION" >&2
    exit 1
  fi
  mv -f "$download" "$jar"
fi
if ! matches_release "$jar"; then
  echo "$jar is not tla2tools.jar v$TLA_TOOLS_VERSION: delete it and rerun" >&2
  exit 1
fi

case "$(docker info --format '{{json .SecurityOptions}}')" in
  *name=rootless*) docker_user=0:0 ;;
  *) docker_user="$(id -u):$(id -g)" ;;
esac

status=0
docker run --rm \
  --user "$docker_user" \
  --memory "$MEMORY" \
  --tmpfs "/meta:size=$WORKING_FILES" \
  --volume "$model_dir:/model:ro" \
  --volume "$config_dir/$config:/run.cfg:ro" \
  --volume "$jar:/tla2tools.jar:ro" \
  --workdir /model \
  --env HOME=/tmp \
  "$JAVA_IMAGE" \
  timeout "$TIME_LIMIT" \
  java "-Xmx$HEAP" -XX:+UseParallelGC -cp /tla2tools.jar tlc2.TLC \
  -workers "$WORKERS" -metadir /meta ${mode[@]+"${mode[@]}"} -config /run.cfg "$model" \
  >"$capture" 2>&1 || status=$?

verdict=""
case "$status" in
  0) verdict=$(grep -oE "^Model checking completed\. No error has been found" "$capture" | head -1 || true)
    if [ -n "$simulate" ] && grep -qE "^Finished in" "$capture"; then
      verdict="No error has been found in $traces traces of up to $depth steps, seed $seed"
    fi ;;
  11 | 12 | 13) verdict=$(grep -oE "^Error: (Invariant [A-Za-z0-9_]+ is violated|Temporal properties were violated|Deadlock reached)" "$capture" | head -1 || true) ;;
esac
verdict=${verdict#Model checking completed. }
verdict=${verdict#Error: }
states=$(grep -E "distinct states found" "$capture" | tail -1 | sed -E 's/.* ([0-9,]+) distinct states found.*/\1/' | tr -d , || true)
count=distinct
if [ -n "$simulate" ]; then
  count=checked
  states=$(grep -E "^Progress: [0-9,]+ states checked" "$capture" | tail -1 | sed -E 's/^Progress: ([0-9,]+) states checked.*/\1/' | tr -d , || true)
fi
path=$(grep -oE "^State [0-9]+: <[A-Za-z0-9_]+(\([^)]*\))?" "$capture" | sed -E 's/^State [0-9]+: <//' | grep -v "^Initial" | paste -sd'>' - || true)
mv -f "$capture" "$output"
if [ -z "$verdict" ]; then
  echo "$run | inconclusive (exit $status), see $output | $count=${states:-?} |"
  exit 1
fi
echo "$run | $verdict | $count=${states:-?} | $path"

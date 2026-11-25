#!/usr/bin/env bash

set -e

case "${PUBLIC_DNS_UNAVAILABLE:-}" in
  1|true) ;;
  *) unset PUBLIC_DNS_UNAVAILABLE ;;
esac

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

LUX_BIN="$(command -v lux || echo "$SCRIPT_DIR/lux/bin/lux")"

if [[ ! -e "${LUX_BIN}" ]]; then
  echo "no lux binary available"
  exit 1
fi

LUX="$LUX_BIN --multiplier=${TIMEOUT_MULTIPLIER:-1000}"

$LUX ${@:-tests/*.lux}

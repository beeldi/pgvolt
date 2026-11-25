#!/usr/bin/env bash
set -uo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: fork/tla/run.sh <folder> [run...]" >&2
  exit 2
fi

here=$(cd "$(dirname "$0")" && pwd)
folder="$here/${1%/}"
shift
if [ ! -d "$folder/runs" ]; then
  echo "$folder has no runs/ folder" >&2
  exit 2
fi

configs=()
if [ "$#" -gt 0 ]; then
  for run in "$@"; do
    configs+=("$folder/runs/$run.cfg")
  done
else
  for config in "$folder"/runs/*.cfg; do
    configs+=("$config")
  done
fi

for config in "${configs[@]}"; do
  model=$(sed -n '1s/^\\\* model: //p' "$config")
  if [ -z "$model" ]; then
    models=$(cd "$folder" && ls *.tla)
    if [ "$(echo "$models" | wc -l | tr -d ' ')" != 1 ]; then
      echo "$config names no model and $folder holds several: add a first line \"\\* model: <Model>.tla\"" >&2
      exit 2
    fi
    model=$models
  fi
  if ! "$here/tlc.sh" "$folder/$model" "$config"; then
    echo "stopped at $config: shrink the bounds or split the run" >&2
    exit 1
  fi
done

#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <target-folder>" >&2
  exit 1
fi

target_folder=$1

if [[ ! -d "$target_folder" ]]; then
  echo "Target folder not found: $target_folder" >&2
  exit 1
fi

for directory in "$target_folder"/*/; do
  [[ -d "$directory" ]] || continue
  (cd "$directory" && python3 page-generator.py)
done
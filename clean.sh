#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="out"

# Ensure directory exists
if [[ ! -d "$OUT_DIR" ]]; then
  echo "Directory '$OUT_DIR' does not exist."
  exit 1
fi

# Delete everything except .gitkeep
find "$OUT_DIR" -mindepth 1 -not -name ".gitkeep" -exec rm -rf {} +

echo "Cleaned '$OUT_DIR' (kept .gitkeep)."

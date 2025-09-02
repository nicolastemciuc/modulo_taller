#!/usr/bin/env bash
# Unzip all ./data/*.zip into DEST_DIR/<zip_basename>/ on THIS MACHINE.
# Run this script on master and each worker (no SSH, no proxy).
# Idempotent: re-unzips only when the .zip changes.

set -euo pipefail

# --- Config ---
DEST_DIR="${DEST_DIR:-/mnt/datasets}"   # change with: DEST_DIR=/some/path ./extract_datasets_local.sh

# Detect project root (git top-level if possible, else script dir)
if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  PROJECT_ROOT="$(git rev-parse --show-toplevel)"
else
  PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
SRC_DIR="${SRC_DIR:-${PROJECT_ROOT}/data}"  # where your .zip files live

# --- Helpers ---
err(){ echo "ERROR: $*" >&2; exit 1; }

checksum_file() {
  local f="$1"
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$f" | awk '{print $1}';
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$f" | awk '{print $1}';
  else md5sum "$f" | awk '{print $1}'; fi
}

ensure_dir() {
  local d="$1"
  if sudo -n true 2>/dev/null; then
    sudo mkdir -p "$d"
    sudo chown "$(id -un)":"$(id -gn)" "$d"
  else
    mkdir -p "$d"
  fi
}

ensure_unzip() {
  command -v unzip >/dev/null 2>&1 && return 0
  echo "==> Installing unzip (apt)"
  if ! command -v apt-get >/dev/null 2>&1; then
    err "unzip missing and apt-get not available. Please install unzip and re-run."
  fi
  sudo apt-get update -y
  sudo apt-get install -y unzip
}

unzip_if_changed() {
  local zip_path="$1" dest_root="$2"
  local base="$(basename "$zip_path" .zip)"
  local out_dir="${dest_root}/${base}"
  local mark="${out_dir}/.unzipped.checksum"
  local sum; sum="$(checksum_file "$zip_path")"

  mkdir -p "$out_dir"
  if [[ -f "$mark" ]] && grep -q "$sum" "$mark" 2>/dev/null; then
    echo "==> Unchanged: $(basename "$zip_path") (skipping)"
    return 0
  fi

  echo "==> Unzipping: $(basename "$zip_path") -> $out_dir"
  unzip -o -q "$zip_path" -d "$out_dir"
  echo "$sum  $(basename "$zip_path")" > "$mark"
}

# --- Run ---
[[ -d "$SRC_DIR" ]] || err "No ./data directory at: $SRC_DIR"
shopt -s nullglob
zips=( "$SRC_DIR"/*.zip )
shopt -u nullglob
[[ ${#zips[@]} -gt 0 ]] || err "No .zip files in $SRC_DIR"

echo "==> Project root: $PROJECT_ROOT"
echo "==> Source zips:  $SRC_DIR"
echo "==> Destination:  $DEST_DIR"

ensure_dir "$DEST_DIR"
ensure_unzip

for zip in "${zips[@]}"; do
  unzip_if_changed "$zip" "$DEST_DIR"
done

echo "==> Done. Extracted under: ${DEST_DIR}/<zip_basename>/"

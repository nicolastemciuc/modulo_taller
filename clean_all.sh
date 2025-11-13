#!/usr/bin/env bash
set -euo pipefail

# Colors
green(){ printf "\033[1;32m%s\033[0m\n" "$*"; }
yellow(){ printf "\033[1;33m%s\033[0m\n" "$*"; }

# Repo root = location of this script
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DATASET_DIR="${REPO_ROOT}/dataset_construction"
TRACE_COLLECTOR="${REPO_ROOT}/trace_collector"
WORKLOADS_LATEST="${REPO_ROOT}/workloads/latest"
SPARK_TRACES="${REPO_ROOT}/../spark_traces"

rm_item() {
  local p="$1"
  if [[ -e "$p" || -L "$p" ]]; then
    echo "  - removing $p"
    rm -rf -- "$p"
  fi
}

rm_glob() {
  local pattern="$1"
  shopt -s nullglob
  for f in $pattern; do
    rm_item "$f"
  done
  shopt -u nullglob
}

green "==> Cleaning dataset_construction outputs"

if [[ -d "$DATASET_DIR" ]]; then
  pushd "$DATASET_DIR" >/dev/null

  # single files
  rm_item flow_extraction.csv
  rm_item feature_extraction.csv
  rm_item historical_information.csv
  rm_item final.csv
  rm_item packet_traces.csv
  rm_item polling_traces  # unused symlink/dir if created

  # PID versions
  rm_glob "feature_extraction_pid"*.csv
  rm_glob "historical_information_pid"*.csv
  rm_glob "final_pid"*.csv

  popd >/dev/null
else
  yellow "dataset_construction not found: $DATASET_DIR"
fi

green "==> Cleaning trace files"

# root-level traces
rm_item "${REPO_ROOT}/polling_traces"
rm_item "${REPO_ROOT}/packet_traces.csv"

# trace_collector/packet_traces.csv  <-- NEW
rm_item "${TRACE_COLLECTOR}/packet_traces.csv"

# event traces
rm_item "${TRACE_COLLECTOR}/event_traces"

green "==> Cleaning workload PID files"
rm_item "${WORKLOADS_LATEST}/pids.txt"

green "==> Cleaning spark_traces"
if [[ -d "$SPARK_TRACES" ]]; then
  echo "  - removing contents of $SPARK_TRACES"
  rm -rf "${SPARK_TRACES}/"*
else
  yellow "spark_traces directory not found at $SPARK_TRACES"
fi

green "Cleanup complete."

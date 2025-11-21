#!/usr/bin/env bash
set -euo pipefail

# Config
WORKLOADS_SCRIPT="workloads/spark/run_spark_workloads.sh"
POLLING_COLLECTOR="trace_collector/polling_traces.py"
PACKET_COLLECTOR="trace_collector/packet_traces.py"
EVENT_COLLECTOR="trace_collector/event_traces.py"

FLOW_EXTRACTION="dataset_construction/flow_extraction.py"        # sudo
FEATURE_EXTRACTION="dataset_construction/feature_extraction.py"  # sudo
HIST_FEATURES="dataset_construction/historical_information.py"
LABELING="dataset_construction/labeling.py"

green(){ printf "\033[1;32m%s\033[0m\n" "$*"; }
yellow(){ printf "\033[1;33m%s\033[0m\n" "$*"; }
red(){ printf "\033[1;31m%s\033[0m\n" "$*"; }

SUDO="sudo"
if [[ $EUID -eq 0 ]]; then
  SUDO=""
else
  sudo -n true 2>/dev/null || yellow "Some steps need sudo; you may be prompted."
fi

need_file(){ [[ -f "$1" ]] || { red "Missing $2 at: $1"; exit 1; }; }
need_file "${WORKLOADS_SCRIPT}" "workloads script"
need_file "${POLLING_COLLECTOR}" "polling collector"
need_file "${PACKET_COLLECTOR}" "packet collector"
need_file "${EVENT_COLLECTOR}" "event collector"
need_file "${FLOW_EXTRACTION}" "flow_extraction.py"
need_file "${FEATURE_EXTRACTION}" "feature_extraction.py"
need_file "${HIST_FEATURES}" "historical_information.py"
need_file "${LABELING}" "labeling.py"

# Process management
PIDS=()
NAMES=()

start_collector() {
  local name="$1" cmdline="$2"
  green "Starting ${name} ..."
  bash -c "${cmdline}" &
  local pid=$!
  PIDS+=("${pid}")
  NAMES+=("${name}")
  echo "${name} PID=${pid}"
}

graceful_stop() {
  local pid="$1" name="$2" timeout="${3:-10}"
  kill -0 "${pid}" 2>/dev/null || { echo "${name} already stopped."; return 0; }
  echo "Stopping ${name} (PID ${pid})..."
  kill -INT "${pid}" 2>/dev/null || true
  for _ in $(seq 1 "${timeout}"); do
    kill -0 "${pid}" 2>/dev/null || { echo "${name} stopped."; return 0; }
    sleep 1
  done
  kill -TERM "${pid}" 2>/dev/null || true
  sleep 2
  kill -0 "${pid}" 2>/dev/null && kill -KILL "${pid}" 2>/dev/null || true
}

cleanup() {
  if ((${#PIDS[@]})); then
    yellow "Cleaning up collectors..."
    for i in "${!PIDS[@]}"; do
      graceful_stop "${PIDS[$i]}" "${NAMES[$i]}" 8 || true
    done
  fi
}
trap cleanup EXIT

# 1) Run workloads
green "==> [1/3] Launching Spark workloads"
bash "${WORKLOADS_SCRIPT}" &
WORKLOADS_PID=$!
echo "Workloads PID=${WORKLOADS_PID}"

# 2) Start collectors
green "==> [2/3] Starting trace collectors"
start_collector "polling_traces" "${SUDO} python3 \"${POLLING_COLLECTOR}\""
start_collector "packet_traces"  "${SUDO} python3 \"${PACKET_COLLECTOR}\""
start_collector "event_traces"   "${SUDO} python3 \"${EVENT_COLLECTOR}\""

echo "Waiting for workloads to complete..."
if ! wait "${WORKLOADS_PID}"; then
  rc=$?
  red "Workloads exited with non-zero status (${rc})."
  exit "${rc}"
fi

green "Workloads finished. Stopping collectors..."
for i in "${!PIDS[@]}"; do
  graceful_stop "${PIDS[$i]}" "${NAMES[$i]}" 10 || true
done
PIDS=()
NAMES=()

# 3) Build the dataset
green "==> [3/3] Building the dataset"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}" && pwd)"
DATASET_DIR="${REPO_ROOT}/dataset_construction"
mkdir -p "${DATASET_DIR}"

# Move/prepare inputs if they landed at repo root
[[ -f "${REPO_ROOT}/packet_traces.csv" ]] && mv -f "${REPO_ROOT}/packet_traces.csv" "${DATASET_DIR}/packet_traces.csv"
if [[ -d "${REPO_ROOT}/polling_traces" && ! -e "${DATASET_DIR}/polling_traces" ]]; then
  ln -s ../polling_traces "${DATASET_DIR}/polling_traces"
fi

pushd "${DATASET_DIR}" >/dev/null

green "-- [3.1] Flow extraction"
${SUDO} python3 "./flow_extraction.py"

green "-- [3.2] Feature extraction"
${SUDO} python3 "./feature_extraction.py"

green "-- [3.3] Historical features"
python3 "./historical_information.py"

green "-- [3.4] Labeling"
python3 "./labeling.py"

popd >/dev/null

green "Pipeline complete"

#!/usr/bin/env bash
set -euo pipefail

# --- Config ---
POLLING_COLLECTOR="trace_collector/polling_traces.py"
PACKET_COLLECTOR="trace_collector/packet_traces.py"
EVENT_COLLECTOR="trace_collector/event_traces.py"

green(){ printf "\033[1;32m%s\033[0m\n" "$*"; }
yellow(){ printf "\033[1;33m%s\033[0m\n" "$*"; }
red(){ printf "\033[1;31m%s\033[0m\n" "$*"; }

SUDO="sudo"
if [[ $EUID -eq 0 ]]; then
  SUDO=""
else
  sudo -n true 2>/dev/null || yellow "Collectors need sudo; you may be prompted."
fi

# --- PID tracking ---
PIDS=()
NAMES=()

start_collector() {
  local name="$1" cmd="$2"
  green "Starting ${name} ..."
  bash -c "${cmd}" &
  local pid=$!
  echo "${name} PID=${pid}"
  PIDS+=("${pid}")
  NAMES+=("${name}")
}

graceful_stop() {
  local pid="$1" name="$2" timeout="${3:-8}"
  kill -0 "${pid}" 2>/dev/null || { echo "${name} already stopped."; return 0; }
  echo "Stopping ${name} (PID ${pid})..."
  kill -INT "${pid}" 2>/dev/null || true

  for _ in $(seq 1 "${timeout}"); do
    kill -0 "${pid}" 2>/dev/null || { echo "${name} stopped."; return 0; }
    sleep 1
  done

  kill -TERM "${pid}" 2>/dev/null || true
  sleep 1
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

# --- Start collectors ---
green "==> Launching trace collectors"
start_collector "polling_traces" "${SUDO} python3 \"${POLLING_COLLECTOR}\""
start_collector "packet_traces"  "${SUDO} python3 \"${PACKET_COLLECTOR}\""
start_collector "event_traces"   "${SUDO} python3 \"${EVENT_COLLECTOR}\" -g"

green "Collectors running. Press Ctrl+C to stop."

# Wait indefinitely (until killed)
wait
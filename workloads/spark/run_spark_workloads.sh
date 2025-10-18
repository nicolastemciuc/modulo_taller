#!/usr/bin/env bash
set -euo pipefail

# ========================
# CONFIG (Spark)
# ========================
SPARK_SUBMIT="${SPARK_SUBMIT:-/opt/spark/bin/spark-submit}"
MASTER_URL="${MASTER_URL:-spark://192.168.60.81:7077}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
TRACE_ROOT="${TRACE_ROOT:-/mnt/extradisk/spark_traces}"

# Optional slow mode parameters
APP_ARGS_KMEANS="${APP_ARGS_KMEANS:-"/mnt/datasets/kmeans/t10k-images.idx3-ubyte 120 10 128 8 3"}"
APP_ARGS_PAGERANK="${APP_ARGS_PAGERANK:-"/mnt/datasets/twitch_gamers/large_twitch_edges.txt 80 512 3"}"

EXECUTOR_CORES="${EXECUTOR_CORES:-1}"
TOTAL_EXECUTOR_CORES="${TOTAL_EXECUTOR_CORES:-3}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"

declare -a PROJECTS=(
  "kmeans    kmeans     org.apache.spark.examples.mllib.KMeansExample"
  "pagerank  page-rank  PageRankExample"
)

WAIT_FOR_COMPLETION=1
DO_BUILD=0

# ========================
# CONFIG (helpers)
# ========================
# tcpdump capture
ENABLE_CAPTURE=0
CAP_IFACE="${CAP_IFACE:-ens3}"
CAP_FILE_NAME="${CAP_FILE_NAME:-packets.log}"
# BPF filter (your example)
CAP_FILTER="${CAP_FILTER:-not port 22 and ip and tcp and ((ip[2:2]-((ip[0]&0x0f)<<2)-((tcp[12]&0xf0)>>2))>0)}"
CAP_CMD="${CAP_CMD:-sudo tcpdump_pfring -ni \"$CAP_IFACE\" -l '$CAP_FILTER'}"

# polling collector
ENABLE_POLL=0
# Command that runs your collector (must accept: PID, -i <ms>, -t <mode>, -o <dir>)
POLL_CMD="${POLL_CMD:-python3 ../../trace_collector/polling_traces/run_polling_traces.py}"
POLL_INTERVAL_MS="${POLL_INTERVAL_MS:-50}"
POLL_TS_MODE="${POLL_TS_MODE:-monotonic_ns}"

# Optional hooks (executed inside RUN_DIR). Leave empty to skip.
PRE_HOOK="${PRE_HOOK:-}"
POST_HOOK="${POST_HOOK:-}"

# ========================
# CLI
# ========================
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build) DO_BUILD=1; shift;;
    --no-wait) WAIT_FOR_COMPLETION=0; shift;;
    --slow)
      EXECUTOR_CORES=1
      TOTAL_EXECUTOR_CORES=1
      APP_ARGS_KMEANS="/mnt/datasets/kmeans/t10k-images.idx3-ubyte 160 10 256 12 5"
      APP_ARGS_PAGERANK="/mnt/datasets/twitch_gamers/large_twitch_edges.txt 120 1024 5"
      shift;;
    --capture) ENABLE_CAPTURE=1; shift;;
    --poll) ENABLE_POLL=1; shift;;
    -h|--help)
      cat <<EOF
Usage: $0 [options]
  --build           Run 'sbt package' before each workload
  --no-wait         Do not wait for completion (helpers keep running; PIDs saved)
  --slow            Heavier args but fewer executors (longer runtime)
  --capture         Start tcpdump_pfring capture for each run
  --poll            Start polling trace collector for Spark driver
  -h, --help        Show this help

Env knobs:
  CAP_IFACE, CAP_FILTER, CAP_CMD, CAP_FILE_NAME
  POLL_CMD, POLL_INTERVAL_MS, POLL_TS_MODE
  PRE_HOOK, POST_HOOK
EOF
      exit 0;;
    *) echo "Unknown option: $1" >&2; exit 1;;
  esac
done

# ========================
# Utils
# ========================
die() { echo "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "Missing '$1'"; }
now_utc() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
ts_dir() { date -u +"%Y%m%dT%H%M%SZ"; }

snap_sysinfo() {
  local dir="$1"
  { echo "# $(now_utc) uname -a"; uname -a; } > "$dir/sysinfo.txt" || true
}

write_metadata() {
  local file="$1" app="$2" klass="$3" start="$4" end="$5" exitcode="$6" cmd="$7" jar="$8" pid="$9"
  {
    echo "{"
    echo "  \"host\": \"$(hostname)\","
    echo "  \"app\": \"${app}\","
    echo "  \"spark_class\": \"${klass}\","
    echo "  \"jar\": \"${jar}\","
    echo "  \"driver_pid\": ${pid},"
    echo "  \"start_utc\": \"${start}\","
    echo "  \"end_utc\": \"${end}\","
    echo "  \"exit_code\": ${exitcode},"
    echo "  \"spark_cmd\": \"$(printf '%s' "$cmd" | sed 's/\\/\\\\/g; s/\"/\\"/g')\""
    echo "}"
  } > "$file"
}

find_jar() {
  local dir="$1"
  find "$dir/target" -type f -name "*.jar" ! -name "*sources.jar" ! -name "*javadoc.jar" | sort | tail -n1
}

# Try to locate the Java driver child PID started by spark-submit
driver_java_pid() {
  local parent="$1"
  local timeout_s=30
  local pid=""
  while (( timeout_s > 0 )); do
    # Prefer a child matching Spark main; fall back to any java child
    pid="$(pgrep -P "$parent" -f 'org\.apache\.spark' || true)"
    [[ -n "$pid" ]] && { echo "$pid"; return 0; }
    pid="$(pgrep -P "$parent" -f 'java' || true)"
    [[ -n "$pid" ]] && { echo "$pid"; return 0; }
    sleep 1; ((timeout_s--))
  done
  # As a last resort, return the parent (spark-submit shell)
  echo "$parent"
}

start_capture() {
  local dir="$1"
  local logfile="$dir/${CAP_FILE_NAME}"
  # Expand CAP_CMD now that RUN_DIR exists (it references $CAP_IFACE / $CAP_FILTER)
  local cmd
  # shellcheck disable=SC2059
  printf -v cmd "%s" "$CAP_CMD"
  echo "[helper:capture] $cmd | tee \"$logfile\""
  bash -c "$cmd" | tee "$logfile" &
  local cap_pid=$!
  echo "$cap_pid" > "$dir/capture.pid"
  echo "$cmd" > "$dir/capture.cmd"
  echo "$cap_pid"
}

start_polling() {
  local dir="$1" target_pid="$2"
  local outdir="$dir/polling"
  mkdir -p "$outdir"
  local cmd="$POLL_CMD $target_pid -i $POLL_INTERVAL_MS -t $POLL_TS_MODE -o \"$outdir\""
  echo "[helper:poll] $cmd"
  bash -c "$cmd" >"$dir/polling.out" 2>"$dir/polling.err" &
  local poll_pid=$!
  echo "$poll_pid" > "$dir/polling.pid"
  echo "$cmd" > "$dir/polling.cmd"
  echo "$poll_pid"
}

stop_pid() {
  local pid="$1" name="$2"
  if kill -0 "$pid" 2>/dev/null; then
    echo "[cleanup] stopping $name ($pid)"
    kill "$pid" 2>/dev/null || true
    # give it a moment, then force if needed
    sleep 1
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
  fi
}

run_one() {
  local app="$1" subdir="$2" klass="$3"
  echo "===== [$app] Run ====="
  [[ "$DO_BUILD" -eq 1 ]] && (cd "$subdir" && sbt -no-colors -batch package)
  need "$SPARK_SUBMIT"
  local jar; jar="$(find_jar "$subdir")"
  [[ -n "$jar" ]] || die "Could not find JAR in $subdir/target"

  mkdir -p "$TRACE_ROOT/$app"
  local RUN_DIR="$TRACE_ROOT/$app/$(ts_dir)"
  mkdir -p "$RUN_DIR"; snap_sysinfo "$RUN_DIR"

  local APP_ARGS=""
  case "$app" in
    kmeans)   APP_ARGS="$APP_ARGS_KMEANS" ;;
    pagerank) APP_ARGS="$APP_ARGS_PAGERANK" ;;
  esac

  local CMD="$SPARK_SUBMIT \
    --class \"$klass\" \
    --master \"$MASTER_URL\" \
    --deploy-mode \"$DEPLOY_MODE\" \
    --executor-memory \"$EXECUTOR_MEMORY\" \
    --executor-cores \"$EXECUTOR_CORES\" \
    --total-executor-cores \"$TOTAL_EXECUTOR_CORES\" \
    \"$jar\" $APP_ARGS"

  # Optional pre-hook (execs inside RUN_DIR)
  if [[ -n "${PRE_HOOK// }" ]]; then
    ( cd "$RUN_DIR" && bash -lc "$PRE_HOOK" ) || echo "[pre-hook] failed (continuing)"
  fi

  echo "[${app}] launching spark-submit ..."
  local START_UTC; START_UTC="$(now_utc)"
  bash -c "$CMD" >"$RUN_DIR/spark.out" 2>"$RUN_DIR/spark.err" &
  local SUBMIT_PID=$!
  echo "$SUBMIT_PID" >"$RUN_DIR/spark_submit.pid"

  # Trap Ctrl-C to clean helpers for this run
  trap 'echo; echo "[signal] SIGINT received"; [[ -f "'"$RUN_DIR"'/capture.pid" ]] && stop_pid "$(cat "'"$RUN_DIR"'/capture.pid")" capture; [[ -f "'"$RUN_DIR"'/polling.pid" ]] && stop_pid "$(cat "'"$RUN_DIR"'/polling.pid")" polling; kill -0 "'"$SUBMIT_PID"'" 2>/dev/null && kill "'"$SUBMIT_PID"'" || true; exit 130' INT

  # Discover the actual driver java PID
  local DRIVER_PID
  DRIVER_PID="$(driver_java_pid "$SUBMIT_PID")"
  echo "$DRIVER_PID" >"$RUN_DIR/driver.pid"
  echo "PID=$DRIVER_PID"

  # Start helpers (capture / polling)
  local CAP_PID=""; local POLL_PID=""
  if [[ "$ENABLE_CAPTURE" -eq 1 ]]; then
    CAP_PID="$(start_capture "$RUN_DIR")"
  fi
  if [[ "$ENABLE_POLL" -eq 1 ]]; then
    POLL_PID="$(start_polling "$RUN_DIR" "$DRIVER_PID")"
  fi

  # Launch and (optionally) wait for completion
  local EXIT=0
  if [[ "$WAIT_FOR_COMPLETION" -eq 1 ]]; then
    wait "$SUBMIT_PID" || EXIT=$?
    # Cleanup helpers on completion
    [[ -n "$CAP_PID"  ]]  && stop_pid "$CAP_PID"  "capture"
    [[ -n "$POLL_PID" ]]  && stop_pid "$POLL_PID" "polling"
  else
    echo "[${app}] running detached. Helper PIDs saved in $RUN_DIR/"
  fi

  local END_UTC; END_UTC="$(now_utc)"
  write_metadata "$RUN_DIR/run.json" "$app" "$klass" "$START_UTC" "$END_UTC" "$EXIT" "$CMD" "$jar" "$DRIVER_PID"
  echo "[${app}] artifacts -> $RUN_DIR"

  # Optional post-hook
  if [[ -n "${POST_HOOK// }" ]]; then
    ( cd "$RUN_DIR" && bash -lc "$POST_HOOK" ) || echo "[post-hook] failed (continuing)"
  fi
}

for row in "${PROJECTS[@]}"; do run_one $row; done
echo "All workloads launched ✔"

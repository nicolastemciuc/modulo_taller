#!/usr/bin/env bash
set -euo pipefail

SPARK_SUBMIT="${SPARK_SUBMIT:-/opt/spark/bin/spark-submit}"
MASTER_URL="${MASTER_URL:-spark://192.168.60.81:7077}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
TRACE_ROOT="${TRACE_ROOT:-/mnt/extradisk/spark_traces}"
# Stable place that lists all current driver PIDs (newline-separated)
PID_FILE="${PID_FILE:-$TRACE_ROOT/latest/driver.pid}"

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
    -h|--help)
      echo "Usage: $0 [--build] [--no-wait] [--slow]"; exit 0;;
    *) echo "Unknown option: $1"; exit 1;;
  esac
done

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

# --- Prepare the stable "latest" space (overwrite PID list for this run) ---
mkdir -p "$(dirname "$PID_FILE")"
: > "$PID_FILE"                        # overwrite the PID file at the beginning of each run of this script
printf '%s\n' "$TRACE_ROOT" > "$(dirname "$PID_FILE")/root"  # optional small breadcrumbs

run_one() {
  local app="$1" subdir="$2" klass="$3"
  echo "===== [$app] Run ====="
  [[ "$DO_BUILD" -eq 1 ]] && (cd "$subdir" && sbt -no-colors -batch package)
  need "$SPARK_SUBMIT"
  local jar; jar="$(find_jar "$subdir")"
  mkdir -p "$TRACE_ROOT/$app"
  local RUN_DIR="$TRACE_ROOT/$app/$(ts_dir)"
  mkdir -p "$RUN_DIR"; snap_sysinfo "$RUN_DIR"

  local APP_ARGS=""
  case "$app" in
    kmeans) APP_ARGS="$APP_ARGS_KMEANS" ;;
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

  echo "[${app}] launching spark-submit ..."
  local START_UTC; START_UTC="$(now_utc)"
  bash -c "$CMD" >"$RUN_DIR/spark.out" 2>"$RUN_DIR/spark.err" &
  local DRIVER_PID=$!
  echo "$DRIVER_PID" >"$RUN_DIR/driver.pid"
  echo "PID=$DRIVER_PID"

  # Append this PID to the stable list (newline-separated)
  printf '%s\n' "$DRIVER_PID" >> "$PID_FILE"
  printf '%s\n' "$RUN_DIR" > "$(dirname "$PID_FILE")/run_dir_${app}"

  if [[ "$WAIT_FOR_COMPLETION" -eq 1 ]]; then
    wait "$DRIVER_PID"; EXIT=$?
  else
    EXIT=0
  fi

  local END_UTC; END_UTC="$(now_utc)"
  write_metadata "$RUN_DIR/run.json" "$app" "$klass" "$START_UTC" "$END_UTC" "$EXIT" "$CMD" "$jar" "$DRIVER_PID"
  echo "[${app}] artifacts -> $RUN_DIR"
}

for row in "${PROJECTS[@]}"; do run_one $row; done
echo "All workloads launched ✔"

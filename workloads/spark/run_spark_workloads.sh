#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Run Spark workloads (driver host only) and expose driver PID
# - Default: runs KMeans + PageRank (SGD disabled by default)
# - Use --build to run `sbt package` first
# - Use --include-sgd to also run the SGD workload
# - Creates: ~/spark_traces/<app>/<UTC timestamp>/
# - Writes the driver PID to: <run_dir>/driver.pid
# - Echoes "PID=<pid>" to STDOUT (so another script can consume it)
# ============================================================

# --------- CONFIG ----------
SPARK_SUBMIT="${SPARK_SUBMIT:-/opt/spark/bin/spark-submit}"
MASTER_URL="${MASTER_URL:-spark://192.168.60.81:7077}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"   # ensure driver runs on this host

TRACE_ROOT="${TRACE_ROOT:-/mnt/extradisk/spark_traces}"

DO_BUILD=0
INCLUDE_SGD=0

# Projects: <name> <subdir> <class>
declare -a PROJECTS=(
  "kmeans    kmeans     org.apache.spark.examples.mllib.KMeansExample"
  "pagerank  page-rank  PageRankExample"
  # "sgd      sgd        org.apache.spark.examples.mllib.SVMWithSGDExample"  # opt-in
)

# --------- CLI ----------
usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --build            Run 'sbt package' for each project before submitting
  --include-sgd      Also run the 'sgd' workload
  --no-wait          Don't wait for Spark to finish (script exits after printing PID)
  -h, --help         Show this help

Environment (optional):
  SPARK_SUBMIT       Path to spark-submit (default: $SPARK_SUBMIT)
  MASTER_URL         Spark master URL      (default: $MASTER_URL)
  DEPLOY_MODE        client|cluster        (default: $DEPLOY_MODE; 'client' recommended)
  TRACE_ROOT         Output root directory (default: $TRACE_ROOT)

Examples:
  ./run_spark_only.sh
  ./run_spark_only.sh --build --include-sgd
  ./run_spark_only.sh --no-wait        # starts Spark in background, prints PID, exits
EOF
}

WAIT_FOR_COMPLETION=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build) DO_BUILD=1; shift;;
    --include-sgd) INCLUDE_SGD=1; shift;;
    --no-wait) WAIT_FOR_COMPLETION=0; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown option: $1"; usage; exit 1;;
  esac
done

# --------- helpers ----------
die() { echo "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "Missing '$1' in PATH"; }
now_utc() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
ts_dir() { date -u +"%Y%m%dT%H%M%SZ"; }

snap_sysinfo() {
  local dir="$1"
  { echo "# $(now_utc) uname -a"; uname -a; } >  "$dir/sysinfo.txt" || true
  { echo "# $(now_utc) lscpu"; lscpu || true; } >> "$dir/sysinfo.txt"
  { echo "# $(now_utc) free -m"; free -m || true; } >> "$dir/sysinfo.txt"
  { echo "# $(now_utc) /proc/meminfo"; cat /proc/meminfo || true; } >> "$dir/sysinfo.txt"
}

write_metadata() {
  local file="$1" app="$2" klass="$3" start="$4" end="$5" exitcode="$6" cmd="$7" jar="$8" pid="$9"
  {
    echo "{"
    echo "  \"host\": \"$(hostname)\","
    echo "  \"app\": \"${app}\","
    echo "  \"spark_class\": \"${klass}\","
    echo "  \"jar\": \"${jar}\","
    echo "  \"master\": \"${MASTER_URL}\","
    echo "  \"deploy_mode\": \"${DEPLOY_MODE}\","
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
  local jar=""
  jar="$(find "$dir/target" -type f \( -name "*assembly*.jar" -o -name "*-fat.jar" \) 2>/dev/null | head -n1 || true)"
  if [[ -z "${jar:-}" ]]; then
    jar="$(find "$dir/target" -type f -name "*.jar" ! -name "*sources.jar" ! -name "*javadoc.jar" 2>/dev/null | sort | tail -n1 || true)"
  fi
  [[ -n "${jar:-}" ]] || die "No jar found in $dir/target. Build with --build."
  echo "$jar"
}

run_one() {
  local app="$1" subdir="$2" klass="$3"
  echo
  echo "===== [${app}] Run ====="

  if [[ "$DO_BUILD" -eq 1 ]]; then
    need sbt
    echo "[${app}] sbt package in ./${subdir}"
    (cd "$subdir" && sbt -no-colors -batch package)
  fi

  need "$SPARK_SUBMIT"
  local jar; jar="$(find_jar "$subdir")"
  echo "[${app}] jar => $jar"

  mkdir -p "$TRACE_ROOT"
  local RUN_TS RUN_DIR SPARK_OUT SPARK_ERR META_JSON PID_FILE
  RUN_TS="$(ts_dir)"
  RUN_DIR="${TRACE_ROOT}/${app}/${RUN_TS}"
  mkdir -p "$RUN_DIR"

  snap_sysinfo "$RUN_DIR"
  SPARK_OUT="${RUN_DIR}/spark.out"
  SPARK_ERR="${RUN_DIR}/spark.err"
  META_JSON="${RUN_DIR}/run.json"
  PID_FILE="${RUN_DIR}/driver.pid"

  local CMD
  CMD="$SPARK_SUBMIT --class \"$klass\" --master \"$MASTER_URL\" --deploy-mode \"$DEPLOY_MODE\" \"$jar\""

  echo "[${app}] launching spark-submit (mode=${DEPLOY_MODE}) ..."
  local START_UTC; START_UTC="$(now_utc)"
  set +e

  if [[ "$WAIT_FOR_COMPLETION" -eq 1 ]]; then
    # Foreground (PID belongs to Java driver process)
    bash -c "$CMD" \
      > >(tee -a \"$SPARK_OUT\") 2> >(tee -a \"$SPARK_ERR\" >&2) &
    DRIVER_PID=$!
    echo "$DRIVER_PID" > "$PID_FILE"
    echo "PID=$DRIVER_PID"   # <- easy to capture by another script
    echo "[${app}] driver_pid=${DRIVER_PID} (logs in $RUN_DIR)"

    # Wait for Spark to finish
    wait "$DRIVER_PID"
    EXIT=$?
  else
    # Fully background; script exits after printing PID
    nohup bash -c "$CMD" \
      > >(tee -a \"$SPARK_OUT\") 2> >(tee -a \"$SPARK_ERR\" >&2) &
    DRIVER_PID=$!
    echo "$DRIVER_PID" > "$PID_FILE"
    echo "PID=$DRIVER_PID"
    echo "[${app}] started in background (driver_pid=${DRIVER_PID})."
    EXIT=0
  fi

  set -e
  local END_UTC; END_UTC="$(now_utc)"

  write_metadata "$META_JSON" "$app" "$klass" "$START_UTC" "$END_UTC" "${EXIT:-0}" \
                 "$SPARK_SUBMIT --class $klass --master $MASTER_URL --deploy-mode $DEPLOY_MODE $jar" "$jar" "$DRIVER_PID"

  if command -v gzip >/dev/null 2>&1; then
    gzip -f -9 "$SPARK_OUT" "$SPARK_ERR" || true
  fi

  echo "[${app}] artifacts -> $RUN_DIR"
}

# --------- Build run list (skip SGD unless requested) ----------
RUN_LIST=()
for row in "${PROJECTS[@]}"; do
  if [[ "$row" == "sgd "* || "$row" == "sgd"* ]]; then
    [[ "$INCLUDE_SGD" -eq 1 ]] || continue
  fi
  RUN_LIST+=("$row")
done

# --------- Preflight & Execute ----------
need "$SPARK_SUBMIT"
mkdir -p "$TRACE_ROOT"

for row in "${RUN_LIST[@]}"; do
  # shellcheck disable=SC2086
  run_one $row
done

echo
echo "All selected workloads launched ✔"
echo "Root folder: $TRACE_ROOT"

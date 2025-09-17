#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Run Spark workloads (driver host only) and collect metrics
# - Default: runs KMeans + PageRank (SGD disabled by default)
# - Use --build to run `sbt package` first
# - Use --include-sgd to also run the SGD workload
# - Saves under: ~/spark_traces/<app>/<UTC timestamp>/
# - Assumes pooling_traces.py already exists (no auto-creation)
# ============================================================

# --------- CONFIG ----------
SPARK_SUBMIT="/opt/spark/bin/spark-submit"
MASTER_URL="${MASTER_URL:-spark://192.168.60.81:7077}"

TRACE_ROOT="${TRACE_ROOT:-$HOME/spark_traces}"
TRACER="${TRACER:-$(dirname "$0")/pooling_traces.py}"  # path to your tracer script
TRACE_INTERVAL="${TRACE_INTERVAL:-0.2}"
TRACE_DEBUG="${TRACE_DEBUG:-0}"  # 1 prints rows to tracer.log as well

DO_BUILD=0
INCLUDE_SGD=0

# Projects: <name> <subdir> <class>
declare -a PROJECTS=(
  "kmeans kmeans org.apache.spark.examples.mllib.KMeansExample"
  "pagerank page-rank PageRankExample"
  # "sgd sgd org.apache.spark.examples.mllib.SVMWithSGDExample"  # disabled by default
)

# --------- CLI ----------
usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --build            Run 'sbt package' for each project before submitting
  --include-sgd      Also run the 'sgd' workload
  -h, --help         Show this help

Environment (optional):
  MASTER_URL         Spark master URL (default: $MASTER_URL)
  TRACE_ROOT         Trace output root (default: $TRACE_ROOT)
  TRACER             Path to pooling_traces.py (default: $TRACER)
  TRACE_INTERVAL     Tracer polling interval seconds (default: $TRACE_INTERVAL)
  TRACE_DEBUG        1 to echo CSV rows into log (default: $TRACE_DEBUG)

Examples:
  ./run_workloads.sh
  ./run_workloads.sh --build
  ./run_workloads.sh --include-sgd
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build) DO_BUILD=1; shift;;
    --include-sgd) INCLUDE_SGD=1; shift;;
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
  local file="$1" app="$2" klass="$3" start="$4" end="$5" exitcode="$6" cmd="$7" jar="$8"
  {
    echo "{"
    echo "  \"host\": \"$(hostname)\","
    echo "  \"app\": \"${app}\","
    echo "  \"spark_class\": \"${klass}\","
    echo "  \"jar\": \"${jar}\","
    echo "  \"master\": \"${MASTER_URL}\","
    echo "  \"start_utc\": \"${start}\","
    echo "  \"end_utc\": \"${end}\","
    echo "  \"exit_code\": ${exitcode},"
    echo "  \"spark_cmd\": \"$(printf '%s' "$cmd" | sed 's/\\/\\\\/g; s/\"/\\"/g')\""
    echo "}"
  } > "$file"
}

# Start tracer fully detached; log stdout+stderr to a file; return PID
start_tracer() {
  local klass="$1" outcsv="$2" log="$3"
  local debug_flag=()
  [[ "$TRACE_DEBUG" == "1" ]] && debug_flag=(-d)

  # ensure log file exists
  : > "$log" || true

  if command -v setsid >/dev/null 2>&1; then
    setsid python3 "$TRACER" --grep "$klass" --interval "$TRACE_INTERVAL" --out "$outcsv" "${debug_flag[@]}" >>"$log" 2>&1 &
  else
    nohup  python3 "$TRACER" --grep "$klass" --interval "$TRACE_INTERVAL" --out "$outcsv" "${debug_flag[@]}" >>"$log" 2>&1 &
  fi
  echo $!
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

  local jar; jar="$(find_jar "$subdir")"
  echo "[${app}] jar => $jar"

  need python3
  [[ -x "$SPARK_SUBMIT" ]] || die "spark-submit not found at $SPARK_SUBMIT"
  mkdir -p "$TRACE_ROOT"

  local RUN_TS RUN_DIR TRACE_CSV TRACER_LOG SPARK_OUT SPARK_ERR META_JSON
  RUN_TS="$(ts_dir)"
  RUN_DIR="${TRACE_ROOT}/${app}/${RUN_TS}"
  mkdir -p "$RUN_DIR"

  snap_sysinfo "$RUN_DIR"
  TRACE_CSV="${RUN_DIR}/trace.csv"
  TRACER_LOG="${RUN_DIR}/tracer.log"
  SPARK_OUT="${RUN_DIR}/spark.out"
  SPARK_ERR="${RUN_DIR}/spark.err"
  META_JSON="${RUN_DIR}/run.json"

  echo "[${app}] starting tracer (grep '${klass}') -> ${TRACE_CSV}"
  local TRACER_PID; TRACER_PID="$(start_tracer "$klass" "$TRACE_CSV" "$TRACER_LOG")"
  echo "[${app}] tracer_pid=${TRACER_PID} (log: ${TRACER_LOG})"

  local START_UTC; START_UTC="$(now_utc)"
  local EXIT=0

  echo "[${app}] launching spark-submit ..."
  set +e
  "$SPARK_SUBMIT" --class "$klass" --master "$MASTER_URL" "$jar" \
    > >(tee -a "$SPARK_OUT") 2> >(tee -a "$SPARK_ERR" >&2)
  EXIT=$?
  set -e

  local END_UTC; END_UTC="$(now_utc)"
  echo "[${app}] spark exit code: $EXIT"

  # Stop tracer gracefully
  if kill -0 "$TRACER_PID" 2>/dev/null; then
    echo "[${app}] stopping tracer (pid $TRACER_PID)"
    kill -INT "$TRACER_PID" || true
    for _ in {1..50}; do
      kill -0 "$TRACER_PID" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$TRACER_PID" 2>/dev/null || true
  fi

  write_metadata "$META_JSON" "$app" "$klass" "$START_UTC" "$END_UTC" "$EXIT" \
                 "$SPARK_SUBMIT --class $klass --master $MASTER_URL $jar" "$jar"

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
[[ -f "$TRACER" ]] || die "Tracer not found at: $TRACER"
mkdir -p "$TRACE_ROOT"

for row in "${RUN_LIST[@]}"; do
  # shellcheck disable=SC2086
  run_one $row
done

echo
echo "All selected workloads finished ✔"
echo "Root folder: $TRACE_ROOT"

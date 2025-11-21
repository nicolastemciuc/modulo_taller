#!/usr/bin/env bash
set -euo pipefail

# --- Base paths relative to script location ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"  # two levels up
cd "${REPO_ROOT}"

# --- Config ---
SPARK_SUBMIT="${SPARK_SUBMIT:-/opt/spark/bin/spark-submit}"
MASTER_URL="${MASTER_URL:-spark://192.168.60.81:7077}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
TRACE_ROOT="${TRACE_ROOT:-${REPO_ROOT}/../spark_traces}"
PID_FILE="${PID_FILE:-${REPO_ROOT}/workloads/latest/pids.txt}"

APP_ARGS_KMEANS="${APP_ARGS_KMEANS:-"/mnt/datasets/kmeans/t10k-images.idx3-ubyte 160 10 256 12 5"}"
APP_ARGS_PAGERANK="${APP_ARGS_PAGERANK:-"/mnt/datasets/twitch_gamers/large_twitch_edges.txt 120 1024 5"}"
APP_ARGS_SVM="${APP_ARGS_SVM:-"/mnt/datasets/criteo/test.txt auto 5000 0.1 1.0 512 ,"}"

EXECUTOR_CORES="${EXECUTOR_CORES:-1}"
TOTAL_EXECUTOR_CORES="${TOTAL_EXECUTOR_CORES:-1}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"

die(){ echo "ERROR: $*" >&2; exit 1; }
need(){ command -v "$1" >/dev/null 2>&1 || die "Missing '$1'"; }
ts_dir(){ date -u +"%Y%m%dT%H%M%SZ"; }

find_jar() {
  local dir="$1"
  find "$dir/target" -type f -name "*.jar" ! -name "*sources.jar" ! -name "*javadoc.jar" | sort | tail -n1
}

run_one() {
  local app="$1" subdir="$2" klass="$3" args="$4"
  need "$SPARK_SUBMIT"
  local jar; jar="$(find_jar "${REPO_ROOT}/workloads/spark/${subdir}")"
  [[ -n "$jar" ]] || die "Jar not found under ${REPO_ROOT}/workloads/spark/${subdir}/target"
  local run_dir="$TRACE_ROOT/$app/$(ts_dir)"
  mkdir -p "$run_dir"
  (
    "$SPARK_SUBMIT" \
      --class "$klass" \
      --master "$MASTER_URL" \
      --deploy-mode "$DEPLOY_MODE" \
      --executor-memory "$EXECUTOR_MEMORY" \
      --executor-cores "$EXECUTOR_CORES" \
      --total-executor-cores "$TOTAL_EXECUTOR_CORES" \
      "$jar" $args
  ) >"$run_dir/spark.out" 2>"$run_dir/spark.err" &
  local pid=$!
  echo "$pid" >"$run_dir/driver.pid"
  echo "$pid" >>"$PID_FILE"
  echo "[$app] PID=$pid logs=$run_dir"
}

mode="parallel"
[[ $# -gt 0 ]] && mode="$1"

mkdir -p "$(dirname "$PID_FILE")"
: > "$PID_FILE"

case "$mode" in
  --kmeans)
    run_one kmeans kmeans org.apache.spark.examples.mllib.KMeansExample "$APP_ARGS_KMEANS"
    wait
    ;;
  --page-rank)
    run_one pagerank page-rank PageRankExample "$APP_ARGS_PAGERANK"
    wait
    ;;
  --svm)
    run_one svm sgd org.apache.spark.examples.mllib.SVMWithSGDExample "$APP_ARGS_SVM"
    wait
    ;;
  --parallel|*)
    run_one kmeans   kmeans     org.apache.spark.examples.mllib.KMeansExample "$APP_ARGS_KMEANS"
    run_one pagerank page-rank  PageRankExample                               "$APP_ARGS_PAGERANK"
    run_one svm sgd org.apache.spark.examples.mllib.SVMWithSGDExample "$APP_ARGS_SVM"
    wait
    ;;
esac

echo "Done. PIDs -> $PID_FILE"

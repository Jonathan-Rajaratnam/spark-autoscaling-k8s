#!/usr/bin/env bash
# run_experiment.sh — Runs all 3 autoscaling strategies sequentially and
# captures executor count, CPU, and memory every 10 seconds into a CSV.
#
# Usage:
#   ./experiment/run_experiment.sh [mode] [environment]
#   ./experiment/run_experiment.sh [environment] [mode]
#
#   mode:
#     top_games | sentiment | user_activity | steam_heavy (default) | property_prices
#
#   environment:
#     local (default) | eks
#
# Prerequisites:
#   - kubectl configured and pointing at your local cluster
#   - kube-prometheus-stack installed (for Prometheus)
#   - KEDA installed (for strategy 2)
#   - Prometheus Adapter installed (for strategy 3)
#   - curl (always available on macOS)

set -euo pipefail

kubectl() {
  if [[ -n "${KUBE_CONTEXT:-}" ]]; then
    command kubectl --context "${KUBE_CONTEXT}" "$@"
  else
    command kubectl "$@"
  fi
}

usage() {
  cat <<'EOF'
Usage:
  ./experiment/run_experiment.sh [mode] [environment]
  ./experiment/run_experiment.sh [environment] [mode]

Modes:
  top_games | sentiment | user_activity | steam_heavy | property_prices

Environments:
  local | eks

Examples:
  ./experiment/run_experiment.sh
  ./experiment/run_experiment.sh property_prices
  ./experiment/run_experiment.sh local
  ./experiment/run_experiment.sh property_prices local
  ./experiment/run_experiment.sh eks steam_heavy
EOF
}

is_valid_mode() {
  case "$1" in
    top_games|sentiment|user_activity|steam_heavy|property_prices) return 0 ;;
    *) return 1 ;;
  esac
}

is_valid_env() {
  case "$1" in
    local|eks) return 0 ;;
    *) return 1 ;;
  esac
}

MODE="steam_heavy"
ENV="local"
HAS_MODE=0
HAS_ENV=0

for arg in "$@"; do
  if is_valid_env "$arg"; then
    if [[ "$HAS_ENV" -eq 1 ]]; then
      echo "Error: environment specified more than once." >&2
      usage >&2
      exit 1
    fi
    ENV="$arg"
    HAS_ENV=1
  elif is_valid_mode "$arg"; then
    if [[ "$HAS_MODE" -eq 1 ]]; then
      echo "Error: mode specified more than once." >&2
      usage >&2
      exit 1
    fi
    MODE="$arg"
    HAS_MODE=1
  else
    echo "Error: unknown argument '$arg'." >&2
    usage >&2
    exit 1
  fi
done

if [[ $# -gt 2 ]]; then
  echo "Error: expected at most 2 arguments, got $#." >&2
  usage >&2
  exit 1
fi

if [[ "$ENV" == "local" ]]; then
  EXT=".yaml"
  HPA_FILE="hpa.yaml"
else
  EXT="-eks.yaml"
  HPA_FILE="hpa-eks.yaml"
fi
RESULTS_DIR="experiment/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
CSV_FILE="${RESULTS_DIR}/${TIMESTAMP}-${MODE}.csv"
SUMMARY_FILE="${RESULTS_DIR}/${TIMESTAMP}-${MODE}-summary.txt"

mkdir -p "$RESULTS_DIR"
echo "strategy,timestamp,executor_count,cpu_millicores,memory_mib" > "$CSV_FILE"
echo -e "\nExperiment: mode=${MODE} env=${ENV} started=$(date)" > "$SUMMARY_FILE"

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }
WARNED_LOCAL_TOP=0
WARNED_PROM_QUERY=0

get_strategy_timeout() {
  local strategy="$1"

  case "${MODE}:${ENV}:${strategy}" in
    property_prices:local:dynamic) echo 3200 ;;
    property_prices:local:keda|property_prices:local:hpa) echo 3200 ;;
    property_prices:eks:dynamic|property_prices:eks:keda|property_prices:eks:hpa) echo 3200 ;;
    steam_heavy:local:dynamic|steam_heavy:local:keda|steam_heavy:local:hpa) echo 900 ;;
    steam_heavy:eks:dynamic|steam_heavy:eks:keda|steam_heavy:eks:hpa) echo 750 ;;
    *) echo 600 ;;
  esac
}

cleanup_strategy_resources() {
  local strategy="$1"
  case "$strategy" in
    dynamic)
      kubectl delete -f k8s/strategies/1-dynamic-allocation/spark-app${EXT} \
        --ignore-not-found >/dev/null 2>&1 || true
      ;;
    keda)
      kubectl delete job/spark-submit-keda --ignore-not-found >/dev/null 2>&1 || true
      kubectl delete -f k8s/strategies/2-keda/keda-scaledobject.yaml \
        --ignore-not-found >/dev/null 2>&1 || true
      kubectl delete -f k8s/strategies/2-keda/spark-worker-deployment${EXT} \
        --ignore-not-found >/dev/null 2>&1 || true
      kubectl delete -f k8s/strategies/2-keda/spark-master.yaml \
        --ignore-not-found >/dev/null 2>&1 || true
      ;;
    hpa)
      kubectl delete job/spark-submit-hpa --ignore-not-found >/dev/null 2>&1 || true
      kubectl delete -f k8s/strategies/3-hpa/${HPA_FILE} \
        --ignore-not-found >/dev/null 2>&1 || true
      kubectl delete -f k8s/strategies/3-hpa/spark-worker-deployment${EXT} \
        --ignore-not-found >/dev/null 2>&1 || true
      kubectl delete -f k8s/strategies/3-hpa/spark-master.yaml \
        --ignore-not-found >/dev/null 2>&1 || true
      ;;
    *)
      echo "Unknown strategy cleanup target: ${strategy}" >&2
      return 1
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Prometheus — accessed via localhost port-forward (started in MAIN)
# ---------------------------------------------------------------------------
PROM_LOCAL="http://127.0.0.1:9091"
PROM_PF_PID=""

start_prom_portforward() {
  log "Starting Prometheus port-forward on 127.0.0.1:9091..."
  kubectl port-forward --address 127.0.0.1 svc/monitoring-kube-prometheus-prometheus \
    9091:9090 -n monitoring &>/dev/null &
  PROM_PF_PID=$!
  sleep 3   # give it time to establish
  log "Prometheus port-forward ready (pid ${PROM_PF_PID})"
}

stop_prom_portforward() {
  if [[ -n "${PROM_PF_PID}" ]]; then
    kill "${PROM_PF_PID}" 2>/dev/null || true
    log "Prometheus port-forward stopped"
  fi
}


# ---------------------------------------------------------------------------
# prom_query <promql>  — returns the integer sum of all instant-query results
# ---------------------------------------------------------------------------
prom_query() {
  local query="$1"
  local encoded
  encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$query")
  local result
  result=$(curl -sf "${PROM_LOCAL}/api/v1/query?query=${encoded}" 2>/dev/null \
    | python3 -c "
import sys,json
data=json.load(sys.stdin)
vals=[float(r['value'][1]) for r in data.get('data',{}).get('result',[])]
print(int(sum(vals))) if vals else print('__NO_DATA__')
" 2>/dev/null || echo 0)
  if [[ "$result" == "__NO_DATA__" ]]; then
    if [[ "$WARNED_PROM_QUERY" -eq 0 ]]; then
      log "WARNING: Prometheus returned no matching resource series; CPU/memory may show as 0"
      WARNED_PROM_QUERY=1
    fi
    echo 0
    return 0
  fi
  echo "$result"
}

get_running_pod_names() {
  local selector="$1"
  kubectl get pods -l "${selector}" \
    --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
}

build_prom_pod_regex() {
  local selector="$1"
  local pods
  pods=$(get_running_pod_names "$selector")

  if [[ -z "$pods" ]]; then
    echo ""
    return 0
  fi

  printf '%s\n' "$pods" | python3 -c '
import re
import sys

pods = [line.strip() for line in sys.stdin if line.strip()]
print("|".join(re.escape(pod) for pod in pods))
'
}

sum_top_resources() {
  local selector="$1"
  local resource="$2"
  local output

  if ! output=$(kubectl top pods -l "${selector}" --no-headers 2>/dev/null); then
    if [[ "$WARNED_LOCAL_TOP" -eq 0 ]]; then
      log "WARNING: 'kubectl top pods' returned no data; install/verify metrics-server for local CPU/memory metrics"
      WARNED_LOCAL_TOP=1
    fi
    echo 0
    return 0
  fi

  if [[ -z "$output" ]]; then
    echo 0
    return 0
  fi

  if [[ "$resource" == "cpu" ]]; then
    printf '%s\n' "$output" | python3 -c '
import sys

total = 0.0
for line in sys.stdin:
    cols = line.split()
    if len(cols) < 2:
        continue
    value = cols[1]
    if value.endswith("m"):
        total += float(value[:-1])
    else:
        total += float(value) * 1000.0
print(int(total))
'
  else
    printf '%s\n' "$output" | python3 -c '
import sys

UNITS = {
    "Ki": 1 / 1024,
    "Mi": 1,
    "Gi": 1024,
    "Ti": 1024 * 1024,
    "K": 1 / (1024 * 1024),
    "M": 1 / 1024,
    "G": 1,
    "T": 1024,
}

total = 0.0
for line in sys.stdin:
    cols = line.split()
    if len(cols) < 3:
        continue
    value = cols[2]
    suffix = next((unit for unit in UNITS if value.endswith(unit)), "")
    number = float(value[:-len(suffix)]) if suffix else float(value)
    total += number * UNITS.get(suffix, 1 / (1024 * 1024))
print(int(total))
'
  fi
}

# ---------------------------------------------------------------------------
# collect_metrics <strategy> <pod_selector>
# Appends one CSV row. Local uses `kubectl top`; EKS uses Prometheus.
# ---------------------------------------------------------------------------
collect_metrics() {
  local strategy="$1"
  local selector="$2"
  local ts
  ts=$(date +%s)

  # Count running pods matching the selector
  local exec_count
  exec_count=$(kubectl get pods -l "${selector}" \
    --field-selector=status.phase=Running \
    --no-headers 2>/dev/null | wc -l | tr -d ' ')

  local cpu_total
  local mem_total
  if [[ "$ENV" == "local" ]]; then
    cpu_total=$(sum_top_resources "$selector" "cpu")
    mem_total=$(sum_top_resources "$selector" "memory")
  else
    local pod_regex
    pod_regex=$(build_prom_pod_regex "$selector")

    if [[ -z "$pod_regex" ]]; then
      cpu_total=0
      mem_total=0
    else
      # Scope Prometheus queries to the currently running pods for the active strategy.
      local prom_filter="namespace=\"default\",pod=~\"${pod_regex}\",container!=\"\",container!=\"POD\""
      local cpu_query="sum(rate(container_cpu_usage_seconds_total{${prom_filter}}[1m])) * 1000"
      local mem_query="sum(container_memory_working_set_bytes{${prom_filter}}) / 1048576"
      cpu_total=$(prom_query "$cpu_query")
      mem_total=$(prom_query "$mem_query")
    fi
  fi

  echo "${strategy},${ts},${exec_count},${cpu_total},${mem_total}" >> "${CSV_FILE}"
}

# ---------------------------------------------------------------------------
# wait_for_job <job-name> <timeout-seconds>
# ---------------------------------------------------------------------------
wait_for_job() {
  local job="$1"
  local timeout="${2:-600}"
  log "Waiting for Job/${job} to complete (timeout: ${timeout}s)..."
  local elapsed=0
  while [[ $elapsed -lt $timeout ]]; do
    local status
    status=$(kubectl get "job/${job}" \
      -o jsonpath='{.status.conditions[-1:].type}' 2>/dev/null || echo "")
    case "$status" in
      Complete)
        log "Job/${job} finished: Complete"
        return 0
        ;;
      Failed)
        log "ERROR: Job/${job} finished: Failed"
        kubectl logs "job/${job}" --tail=50 2>/dev/null || true
        return 1
        ;;
    esac
    sleep 10
    elapsed=$((elapsed + 10))
  done
  log "ERROR: Job/${job} timed out after ${timeout}s"
  kubectl logs "job/${job}" --tail=50 2>/dev/null || true
  return 1
}

# ---------------------------------------------------------------------------
# wait_for_sparkapplication <name> <timeout-seconds>
# ---------------------------------------------------------------------------
wait_for_sparkapplication() {
  local name="$1"
  local timeout="${2:-600}"
  log "Waiting for SparkApplication/${name} to complete (timeout: ${timeout}s)..."
  local elapsed=0
  local state="UNKNOWN"
  while [[ $elapsed -lt $timeout ]]; do
    state=$(kubectl get sparkapplication "$name" \
      -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "UNKNOWN")
    if [[ "$state" == "COMPLETED" ]]; then
      log "SparkApplication ${name} finished: ${state}"
      return 0
    fi
    if [[ "$state" == "FAILED" ]]; then
      log "ERROR: SparkApplication ${name} finished: ${state}"
      kubectl logs -f "${name}-driver" --tail=50 2>/dev/null || true
      return 1
    fi
    sleep 10
    elapsed=$((elapsed + 10))
  done
  log "ERROR: SparkApplication ${name} timed out after ${timeout}s (last state: ${state})"
  kubectl logs -f "${name}-driver" --tail=50 2>/dev/null || true
  return 1
}

# ---------------------------------------------------------------------------
# poll_metrics <strategy> <selector> <poll_interval>
# Runs in the background — polls until signalled by flag file
# ---------------------------------------------------------------------------
poll_metrics() {
  local strategy="$1"
  local selector="$2"
  local interval="${3:-10}"
  local flag="/tmp/poll_${strategy}.run"
  local seen_running=0
  touch "$flag"
  while [[ -f "$flag" ]]; do
    local running
    running=$(kubectl get pods -l "${selector}" \
      --field-selector=status.phase=Running \
      --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$running" -gt 0 ]]; then
      seen_running=1
    fi
    if [[ "$seen_running" -eq 1 ]]; then
      collect_metrics "$strategy" "$selector"
    fi
    sleep "$interval"
  done
}

# ---------------------------------------------------------------------------
# STRATEGY 1 — Dynamic Allocation via SparkApplication CRD
# ---------------------------------------------------------------------------
run_dynamic() {
  echo -e "\n=== STRATEGY 1: Dynamic Allocation ===\n" >&2
  cleanup_strategy_resources "dynamic"
  local start_ts
  local timeout
  start_ts=$(date +%s)
  timeout=$(get_strategy_timeout "dynamic")

  # Patch the mode argument into the manifest
  sed "s/\"steam_heavy\"/\"${MODE}\"/" \
    k8s/strategies/1-dynamic-allocation/spark-app${EXT} \
    | kubectl apply -f -

  # Start background polling
  poll_metrics "dra" "app-role=spark-executor,strategy=dynamic-allocation" 10 &
  local poll_pid=$!

  wait_for_sparkapplication "spark-app-dynamic" "$timeout"

  # Stop polling
  rm -f /tmp/poll_dra.run
  wait "$poll_pid" 2>/dev/null || true

  local end_ts
  end_ts=$(date +%s)
  local duration=$((end_ts - start_ts))
  echo "dra: duration=${duration}s" >> "$SUMMARY_FILE"
  echo -e "\nStrategy 1 done. Duration: ${duration}s\n" >&2

  cleanup_strategy_resources "dynamic"
  log "Cooling down 30s before next strategy..."
  sleep 30
}

# ---------------------------------------------------------------------------
# STRATEGY 2 — KEDA (Spark Standalone + ScaledObject)
# ---------------------------------------------------------------------------
run_keda() {
  echo -e "\n=== STRATEGY 2: KEDA ===\n" >&2
  cleanup_strategy_resources "keda"
  local start_ts
  local timeout
  start_ts=$(date +%s)
  timeout=$(get_strategy_timeout "keda")

  kubectl apply -f k8s/strategies/2-keda/spark-master.yaml
  kubectl apply -f k8s/strategies/2-keda/spark-worker-deployment${EXT}
  kubectl apply -f k8s/strategies/2-keda/keda-scaledobject.yaml

  log "Waiting for KEDA master to be ready..."
  kubectl rollout status deployment/spark-master-keda --timeout=120s
  log "Waiting for KEDA workers to be ready..."
  kubectl rollout status deployment/spark-worker-keda --timeout=120s

  # Patch mode into job and apply
  kubectl delete job/spark-submit-keda --ignore-not-found >/dev/null 2>&1 || true
  sed "s/steam_heavy/${MODE}/" \
    k8s/strategies/2-keda/spark-job${EXT} \
    | kubectl apply -f -

  # Start background polling
  poll_metrics "keda" "app=spark-worker,strategy=keda" 10 &
  local poll_pid=$!

  wait_for_job "spark-submit-keda" "$timeout"

  rm -f /tmp/poll_keda.run
  wait "$poll_pid" 2>/dev/null || true

  local end_ts
  end_ts=$(date +%s)
  local duration=$((end_ts - start_ts))
  echo "keda: duration=${duration}s" >> "$SUMMARY_FILE"
  echo -e "\nStrategy 2 done. Duration: ${duration}s\n" >&2

  cleanup_strategy_resources "keda"
  log "Cooling down 30s before next strategy..."
  sleep 30
}

# ---------------------------------------------------------------------------
# STRATEGY 3 — HPA (Spark Standalone + HorizontalPodAutoscaler)
# ---------------------------------------------------------------------------
run_hpa() {
  echo -e "\n=== STRATEGY 3: HPA ===\n" >&2
  cleanup_strategy_resources "hpa"
  local start_ts
  local timeout
  start_ts=$(date +%s)
  timeout=$(get_strategy_timeout "hpa")

  kubectl apply -f k8s/strategies/3-hpa/spark-master.yaml
  kubectl apply -f k8s/strategies/3-hpa/spark-worker-deployment${EXT}
  kubectl apply -f k8s/strategies/3-hpa/${HPA_FILE}

  log "Waiting for HPA master to be ready..."
  kubectl rollout status deployment/spark-master-hpa --timeout=120s
  log "Waiting for HPA workers to be ready..."
  kubectl rollout status deployment/spark-worker-hpa --timeout=120s

  # Patch mode into job and apply
  kubectl delete job/spark-submit-hpa --ignore-not-found >/dev/null 2>&1 || true
  sed "s/steam_heavy/${MODE}/" \
    k8s/strategies/3-hpa/spark-job${EXT} \
    | kubectl apply -f -

  # Start background polling
  poll_metrics "hpa" "app=spark-worker,strategy=hpa" 10 &
  local poll_pid=$!

  wait_for_job "spark-submit-hpa" "$timeout"

  rm -f /tmp/poll_hpa.run
  wait "$poll_pid" 2>/dev/null || true

  local end_ts
  end_ts=$(date +%s)
  local duration=$((end_ts - start_ts))
  echo "hpa: duration=${duration}s" >> "$SUMMARY_FILE"
  echo -e "\nStrategy 3 done. Duration: ${duration}s\n" >&2

  cleanup_strategy_resources "hpa"
}

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
trap stop_prom_portforward EXIT

log "Starting benchmark experiment: mode=${MODE}, env=${ENV}"
log "Results will be saved to: ${CSV_FILE}"

start_prom_portforward

run_dynamic
run_keda
run_hpa

echo "" >> "$SUMMARY_FILE"
echo "Experiment complete: $(date)" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

log "Writing summary metrics to ${SUMMARY_FILE}..."
python3 experiment/cost_model.py "${CSV_FILE}" >> "${SUMMARY_FILE}"

echo -e "\n=== Experiment complete ===\n" >&2
log "CSV:     ${CSV_FILE}"
log "Summary: ${SUMMARY_FILE}"
log "Run cost model:"
log "  python3 experiment/cost_model.py ${CSV_FILE}"

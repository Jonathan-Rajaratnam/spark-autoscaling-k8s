#!/usr/bin/env bash
# run_experiment.sh — Runs all 3 autoscaling strategies sequentially and
# captures executor count, CPU, and memory every 10 seconds into a CSV.
#
# Usage:
#   ./experiment/run_experiment.sh [mode]
#   mode: steam_heavy (default) | property_prices
#
# Prerequisites:
#   - kubectl configured and pointing at your local cluster
#   - kube-prometheus-stack installed (for Prometheus)
#   - KEDA installed (for strategy 2)
#   - Prometheus Adapter installed (for strategy 3)
#   - curl (always available on macOS)

set -euo pipefail

MODE="${1:-steam_heavy}"
RESULTS_DIR="experiment/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
CSV_FILE="${RESULTS_DIR}/${TIMESTAMP}-${MODE}.csv"
SUMMARY_FILE="${RESULTS_DIR}/${TIMESTAMP}-${MODE}-summary.txt"

mkdir -p "$RESULTS_DIR"
echo "strategy,timestamp,executor_count,cpu_millicores,memory_mib" > "$CSV_FILE"
echo "Experiment: mode=${MODE}  started=$(date)" > "$SUMMARY_FILE"

log() { echo "[$(date +%H:%M:%S)] $*"; }

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
  curl -sf "${PROM_LOCAL}/api/v1/query?query=${encoded}" 2>/dev/null \
    | python3 -c "
import sys,json
data=json.load(sys.stdin)
vals=[float(r['value'][1]) for r in data.get('data',{}).get('result',[])]
print(int(sum(vals))) if vals else print(0)
" 2>/dev/null || echo 0
}

# ---------------------------------------------------------------------------
# collect_metrics <strategy> <pod_selector>
# Appends one CSV row. CPU from Prometheus cAdvisor, memory from working set.
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

  # CPU millicores via Prometheus (cAdvisor — guaranteed present)
  local cpu_query="sum(rate(container_cpu_usage_seconds_total{namespace=\"default\",container!=\"\",container!=\"POD\"}[1m])) * 1000"
  local cpu_total
  cpu_total=$(prom_query "$cpu_query")

  # Memory MiB via Prometheus (working set — excludes cache, matches kubectl top)
  local mem_query="sum(container_memory_working_set_bytes{namespace=\"default\",container!=\"\",container!=\"POD\"}) / 1048576"
  local mem_total
  mem_total=$(prom_query "$mem_query")

  echo "${strategy},${ts},${exec_count},${cpu_total},${mem_total}" >> "${CSV_FILE}"
}

# ---------------------------------------------------------------------------
# wait_for_job <job-name> <timeout-seconds>
# ---------------------------------------------------------------------------
wait_for_job() {
  local job="$1"
  local timeout="${2:-600}"
  log "Waiting for Job/${job} to complete (timeout: ${timeout}s)..."
  kubectl wait --for=condition=complete "job/${job}" \
    --timeout="${timeout}s" 2>/dev/null \
    || kubectl wait --for=condition=failed "job/${job}" \
    --timeout=10s 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# wait_for_sparkapplication <name> <timeout-seconds>
# ---------------------------------------------------------------------------
wait_for_sparkapplication() {
  local name="$1"
  local timeout="${2:-600}"
  log "Waiting for SparkApplication/${name} to complete..."
  local elapsed=0
  while [[ $elapsed -lt $timeout ]]; do
    local state
    state=$(kubectl get sparkapplication "$name" \
      -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "UNKNOWN")
    if [[ "$state" == "COMPLETED" ]] || [[ "$state" == "FAILED" ]]; then
      log "SparkApplication ${name} finished: ${state}"
      return
    fi
    sleep 10
    elapsed=$((elapsed + 10))
  done
  log "WARNING: SparkApplication ${name} timed out after ${timeout}s"
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
  touch "$flag"
  while [[ -f "$flag" ]]; do
    collect_metrics "$strategy" "$selector"
    sleep "$interval"
  done
}

# ---------------------------------------------------------------------------
# STRATEGY 1 — Dynamic Allocation via SparkApplication CRD
# ---------------------------------------------------------------------------
run_dynamic() {
  log "=== STRATEGY 1: Dynamic Allocation ==="
  local start_ts
  start_ts=$(date +%s)

  # Patch the mode argument into the manifest
  sed "s/\"steam_heavy\"/\"${MODE}\"/" \
    k8s/strategies/1-dynamic-allocation/spark-app.yaml \
    | kubectl apply -f -

  # Start background polling
  poll_metrics "dynamic" "app-role=spark-executor,strategy=dynamic-allocation" 10 &
  local poll_pid=$!

  wait_for_sparkapplication "spark-app-dynamic"

  # Stop polling
  rm -f /tmp/poll_dynamic.run
  wait "$poll_pid" 2>/dev/null || true

  local end_ts
  end_ts=$(date +%s)
  local duration=$((end_ts - start_ts))
  echo "dynamic: duration=${duration}s" >> "$SUMMARY_FILE"
  log "Strategy 1 done. Duration: ${duration}s"

  kubectl delete -f k8s/strategies/1-dynamic-allocation/spark-app.yaml --ignore-not-found
  log "Cooling down 30s before next strategy..."
  sleep 30
}

# ---------------------------------------------------------------------------
# STRATEGY 2 — KEDA (Spark Standalone + ScaledObject)
# ---------------------------------------------------------------------------
run_keda() {
  log "=== STRATEGY 2: KEDA ==="
  local start_ts
  start_ts=$(date +%s)

  kubectl apply -f k8s/strategies/2-keda/spark-master.yaml
  kubectl apply -f k8s/strategies/2-keda/spark-worker-deployment.yaml
  kubectl apply -f k8s/strategies/2-keda/keda-scaledobject.yaml

  log "Waiting for KEDA master to be ready..."
  kubectl rollout status deployment/spark-master-keda --timeout=120s

  # Patch mode into job and apply
  sed "s/steam_heavy/${MODE}/" \
    k8s/strategies/2-keda/spark-job.yaml \
    | kubectl apply -f -

  # Start background polling
  poll_metrics "keda" "app=spark-worker,strategy=keda" 10 &
  local poll_pid=$!

  wait_for_job "spark-submit-keda"

  rm -f /tmp/poll_keda.run
  wait "$poll_pid" 2>/dev/null || true

  local end_ts
  end_ts=$(date +%s)
  local duration=$((end_ts - start_ts))
  echo "keda: duration=${duration}s" >> "$SUMMARY_FILE"
  log "Strategy 2 done. Duration: ${duration}s"

  kubectl delete -f k8s/strategies/2-keda/spark-job.yaml --ignore-not-found
  kubectl delete -f k8s/strategies/2-keda/keda-scaledobject.yaml --ignore-not-found
  kubectl delete -f k8s/strategies/2-keda/spark-worker-deployment.yaml --ignore-not-found
  kubectl delete -f k8s/strategies/2-keda/spark-master.yaml --ignore-not-found
  log "Cooling down 30s before next strategy..."
  sleep 30
}

# ---------------------------------------------------------------------------
# STRATEGY 3 — HPA (Spark Standalone + HorizontalPodAutoscaler)
# ---------------------------------------------------------------------------
run_hpa() {
  log "=== STRATEGY 3: HPA ==="
  local start_ts
  start_ts=$(date +%s)

  kubectl apply -f k8s/strategies/3-hpa/spark-master.yaml
  kubectl apply -f k8s/strategies/3-hpa/spark-worker-deployment.yaml
  kubectl apply -f k8s/strategies/3-hpa/hpa.yaml

  log "Waiting for HPA master to be ready..."
  kubectl rollout status deployment/spark-master-hpa --timeout=120s

  # Patch mode into job and apply
  sed "s/steam_heavy/${MODE}/" \
    k8s/strategies/3-hpa/spark-job.yaml \
    | kubectl apply -f -

  # Start background polling
  poll_metrics "hpa" "app=spark-worker,strategy=hpa" 10 &
  local poll_pid=$!

  wait_for_job "spark-submit-hpa"

  rm -f /tmp/poll_hpa.run
  wait "$poll_pid" 2>/dev/null || true

  local end_ts
  end_ts=$(date +%s)
  local duration=$((end_ts - start_ts))
  echo "hpa: duration=${duration}s" >> "$SUMMARY_FILE"
  log "Strategy 3 done. Duration: ${duration}s"

  kubectl delete -f k8s/strategies/3-hpa/spark-job.yaml --ignore-not-found
  kubectl delete -f k8s/strategies/3-hpa/hpa.yaml --ignore-not-found
  kubectl delete -f k8s/strategies/3-hpa/spark-worker-deployment.yaml --ignore-not-found
  kubectl delete -f k8s/strategies/3-hpa/spark-master.yaml --ignore-not-found
}

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
trap stop_prom_portforward EXIT

log "Starting benchmark experiment: mode=${MODE}"
log "Results will be saved to: ${CSV_FILE}"

start_prom_portforward

run_dynamic
run_keda
run_hpa

echo "" >> "$SUMMARY_FILE"
echo "Experiment complete: $(date)" >> "$SUMMARY_FILE"

log ""
log "=== Experiment complete ==="
log "CSV:     ${CSV_FILE}"
log "Summary: ${SUMMARY_FILE}"
log "Run cost model:"
log "  python3 experiment/cost_model.py ${CSV_FILE}"

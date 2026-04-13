#!/usr/bin/env bash
# run_experiment.sh — Runs all 3 autoscaling strategies sequentially and
# captures executor count, CPU, and memory every 10 seconds into a CSV.
#
# Usage:
#   ./experiment/run_experiment.sh [mode]
#   mode: top_games (default) | sentiment | user_activity
#
# Prerequisites:
#   - kubectl configured and pointing at your local cluster
#   - kube-prometheus-stack installed (for Prometheus)
#   - KEDA installed (for strategy 2)
#   - Prometheus Adapter installed (for strategy 3)
#   - metrics-server installed (for kubectl top)

set -euo pipefail

MODE="${1:-top_games}"
RESULTS_DIR="experiment/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
CSV_FILE="${RESULTS_DIR}/${TIMESTAMP}-${MODE}.csv"
SUMMARY_FILE="${RESULTS_DIR}/${TIMESTAMP}-${MODE}-summary.txt"

mkdir -p "$RESULTS_DIR"
echo "strategy,timestamp,executor_count,cpu_millicores,memory_mib" > "$CSV_FILE"
echo "Experiment: mode=${MODE}  started=$(date)" > "$SUMMARY_FILE"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ---------------------------------------------------------------------------
# collect_metrics <strategy> <pod_selector_label_value>
# Appends one CSV row using kubectl top (requires metrics-server)
# ---------------------------------------------------------------------------
collect_metrics() {
  local strategy="$1"
  local selector="$2"
  local ts
  ts=$(date +%s)

  local cpu_total=0
  local mem_total=0
  local exec_count=0

  # Count running pods matching the selector
  exec_count=$(kubectl get pods -l "$selector" \
    --field-selector=status.phase=Running \
    --no-headers 2>/dev/null | wc -l | tr -d ' ')

  # Aggregate CPU + memory from kubectl top
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    local cpu mem
    cpu=$(echo "$line" | awk '{print $2}' | tr -d 'm')
    mem=$(echo "$line" | awk '{print $3}' | tr -d 'Mi')
    cpu_total=$((cpu_total + ${cpu:-0}))
    mem_total=$((mem_total + ${mem:-0}))
  done < <(kubectl top pods -l "$selector" --no-headers 2>/dev/null || true)

  echo "${strategy},${ts},${exec_count},${cpu_total},${mem_total}" >> "$CSV_FILE"
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
  sed "s/\"top_games\"/\"${MODE}\"/" \
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
  sed "s/top_games/${MODE}/" \
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
  sed "s/top_games/${MODE}/" \
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
log "Starting benchmark experiment: mode=${MODE}"
log "Results will be saved to: ${CSV_FILE}"

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

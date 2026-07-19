#!/usr/bin/env bash
# Captures the evidence for the live run: bridge Prometheus metrics, pod
# counts over time, achieved churn (reads scripts/30-churn.sh's CSV output),
# controller RSS (reuses tools/bridge-top.sh's query style), and Kueue
# admission metrics. Writes everything into results/ as text/CSV so the
# orchestrator can commit it after the run.
#
# Metric names below are the REAL ones exported by
# internal/metrics/metrics.go (grepped, not invented):
#   k8s_bridge_tick_duration_seconds        (histogram)
#   k8s_bridge_ticks_total
#   k8s_bridge_tick_errors_total
#   k8s_bridge_tick_trigger_total{source=...}
#   k8s_bridge_jobsets_created_total
#   k8s_bridge_jobsets_deleted_total
#   k8s_bridge_jobset_create_errors_total
#   k8s_bridge_jobs_failed_total
#   k8s_bridge_held_jobs
#   k8s_bridge_slurm_api_requests_total
#   k8s_bridge_job_release_latency_seconds  (histogram)
# Served on the same mux as /healthz, /readyz at --metrics-addr (default
# :8080, see deploy/chart/k8s-bridge/templates/deployment.yaml).
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

RESULTS_DIR="$(dirname "$0")/../results"
mkdir -p "${RESULTS_DIR}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="${RESULTS_DIR}/measure-${TS}.txt"

BRIDGE_POD="$(kubectl -n "${BRIDGE_NAMESPACE}" get pods -l app.kubernetes.io/name=k8s-bridge -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"

{
  echo "=== k8s-bridge scale + GPU-churn measurement — ${TS} ==="
  echo

  echo "--- Cluster node counts by pool ---"
  kubectl get nodes -L pool --no-headers 2>/dev/null | awk '{print $NF}' | sort | uniq -c

  echo
  echo "--- Pod counts ---"
  echo "RUNNING slurmd pods (namespace ${BRIDGE_NAMESPACE}):"
  kubectl get pods -n "${BRIDGE_NAMESPACE}" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l
  echo "Kueue Workloads admitted (RUNNING target: ${RUNNING_TARGET}):"
  kubectl get workloads -A -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Admitted")].status}{"\n"}{end}' 2>/dev/null | grep -c True || true
  echo "Kueue Workloads pending (PENDING target: ${PENDING_TARGET}):"
  kubectl get workloads -A -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Admitted")].status}{"\n"}{end}' 2>/dev/null | grep -vc True || true

  echo
  echo "--- Bridge Prometheus metrics (raw scrape) ---"
  if [ -n "${BRIDGE_POD}" ]; then
    kubectl -n "${BRIDGE_NAMESPACE}" exec "${BRIDGE_POD}" -- wget -qO- http://localhost:8080/metrics 2>/dev/null \
      | grep -E '^k8s_bridge_(tick_duration_seconds|ticks_total|tick_errors_total|tick_trigger_total|jobsets_created_total|jobsets_deleted_total|jobset_create_errors_total|jobs_failed_total|held_jobs|slurm_api_requests_total|job_release_latency_seconds)' \
      || echo "(no matching k8s_bridge_* series found — check the bridge pod is ready and scraping)"
  else
    echo "(bridge pod not found in namespace ${BRIDGE_NAMESPACE} — is it deployed?)"
  fi

  echo
  echo "--- Controller RSS (reusing tools/bridge-top.sh's kubectl-top query style) ---"
  kubectl -n "${BRIDGE_NAMESPACE}" top pod -l app.kubernetes.io/name=k8s-bridge --no-headers 2>/dev/null \
    || echo "(metrics-server top unavailable — enable it or read RSS via /metrics' process_resident_memory_bytes if exposed)"

  echo
  echo "--- Kueue admission metrics (from kueue-controller-manager) ---"
  KUEUE_POD="$(kubectl -n kueue-system get pods -l control-plane=controller-manager -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -n "${KUEUE_POD}" ]; then
    kubectl -n kueue-system exec "${KUEUE_POD}" -- wget -qO- --no-check-certificate https://localhost:8443/metrics 2>/dev/null \
      | grep -E '^kueue_(pending_workloads|admitted_active_workloads|admission_wait_time_seconds|evicted_workloads_total|cluster_queue_resource_usage)' \
      || echo "(metrics endpoint needs auth in-cluster; fall back to: kubectl get clusterqueues -o yaml for .status)"
  else
    echo "(kueue-controller-manager pod not found)"
  fi
  echo "ClusterQueue status snapshot:"
  kubectl get clusterqueues -o custom-columns='NAME:.metadata.name,PENDING:.status.pendingWorkloads,ADMITTED:.status.admittedWorkloads' 2>/dev/null || true

  echo
  echo "--- Achieved churn (from scripts/30-churn.sh CSV, most recent file) ---"
  LATEST_CHURN_CSV="$(ls -t "${RESULTS_DIR}"/churn-*.csv 2>/dev/null | head -1 || true)"
  if [ -n "${LATEST_CHURN_CSV}" ]; then
    echo "Using: ${LATEST_CHURN_CSV}"
    tail -n 20 "${LATEST_CHURN_CSV}"
  else
    echo "(no churn CSV found yet — run scripts/30-churn.sh first)"
  fi

  echo
  echo "--- Grafana dashboard-render check ---"
  echo "Manual/optional step: if GRAFANA_URL is set and reachable, curl its"
  echo "dashboard JSON model to confirm the panels resolve (does not render"
  echo "pixels, just confirms the datasource/query wiring is live):"
  echo '  curl -s "$GRAFANA_URL/api/dashboards/uid/wm-researchers" | head -c 500'
  echo
  echo "Embedding note (see tools/demo-console/README.md, 'What was validated"
  echo "locally'): Grafana blocks <iframe> embedding by default. For the demo"
  echo "console's dashboard pane to show live panels during this run, the"
  echo "Grafana server needs [security] allow_embedding = true (and typically"
  echo "[auth.anonymous] enabled = true for a no-login iframe view) in"
  echo "grafana.ini — this is a Grafana server-side config, not something this"
  echo "script can set from outside. Verify it before relying on the iframe"
  echo "live; a blank panel with no console error usually means this flag."
  if [ -n "${GRAFANA_URL:-}" ]; then
    echo
    echo "GRAFANA_URL is set — attempting dashboard JSON fetch:"
    curl -s --max-time 5 "${GRAFANA_URL%/}/api/dashboards/uid/wm-researchers" | head -c 500 || echo "(fetch failed)"
    echo
  fi

} | tee "${OUT}"

echo
echo "==> Full report written to ${OUT}"

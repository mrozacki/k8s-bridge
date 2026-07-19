#!/usr/bin/env bash
# Teardown script for Experiment 11 scale validation objects.
#
# Order matters: cancel the Slurm queue FIRST. The bridge recreates a JobSet
# for every held job it still sees, so deleting JobSets while thousands of
# jobs sit in Slurm turns teardown into a fight against the reconcile loop
# (suite-E findings #6/#7).
#
# Two speeds:
#   default           - server-side DeleteCollection per resource type (one
#                       request per type, no client-side rate limiting), then
#                       poll until counts reach zero.
#   PURGE_NAMESPACE=1 - delete-and-recreate the whole slurm-jobs namespace
#                       (suite-E finding #10: at tens of thousands of objects
#                       even per-type deletion stalls on the API server;
#                       namespace deletion delegates the cascade to the
#                       server-side garbage collector). This ALSO removes the
#                       bridge install and LocalQueues; the script re-applies
#                       what it dumped and reminds about the helm reinstall.
set -euo pipefail

NS="slurm-jobs"
WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-900}"

LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -n "$LOGIN_POD" ]; then
  echo "==> Cancelling all Slurm jobs first (stops the bridge recreating JobSets)..."
  kubectl exec -n slurm "$LOGIN_POD" -- scancel -u root || true
fi

echo "==> Deleting test deployments in $NS..."
kubectl delete deployment churn-generator -n "$NS" --ignore-not-found

if [ "${PURGE_NAMESPACE:-0}" = "1" ]; then
  DUMP_DIR="$HOME/k8s-bridge-testrun/ns-dump-$(date +%s)"
  mkdir -p "$DUMP_DIR"
  echo "==> PURGE_NAMESPACE=1: dumping LocalQueues and WorkloadMixings to $DUMP_DIR..."
  kubectl get localqueues -n "$NS" -o yaml > "$DUMP_DIR/localqueues.yaml" 2>/dev/null || true
  kubectl get workloadmixings -n "$NS" -o yaml > "$DUMP_DIR/workloadmixings.yaml" 2>/dev/null || true
  echo "==> Deleting namespace $NS (server-side GC does the mass cleanup)..."
  kubectl delete namespace "$NS" --wait=true --timeout="${WAIT_TIMEOUT_SECS}s"
  echo "==> Recreating namespace and restoring dumped resources..."
  kubectl create namespace "$NS"
  for f in "$DUMP_DIR"/*.yaml; do
    # A dump with zero objects is still a non-empty file (an empty List);
    # kubectl apply errors on it, which would kill the script under set -e.
    [ -s "$f" ] && kubectl apply -f "$f" || echo "    (skipped $f: nothing to restore)"
  done
  echo "==> NOTE: the k8s-bridge install lived in $NS and is GONE. Reinstall with:"
  echo "    helm upgrade --install k8s-bridge deploy/chart/k8s-bridge -n $NS \\"
  echo "      -f deploy/chart/k8s-bridge/values-gke-test.yaml --set image.repository=... --set image.tag=..."
else
  echo "==> Mass deletion of jobsets, workloads and jobs via server-side DeleteCollection..."
  # Port 0 lets the kernel pick a free port, so the DELETEs can never hit a
  # foreign listener; the EXIT trap guarantees cleanup even if a curl fails,
  # and `kill ... || true` keeps a dead proxy from aborting the teardown
  # under `set -e`.
  PROXY_LOG=$(mktemp)
  kubectl proxy -p 0 > "$PROXY_LOG" 2>&1 & PROXY_PID=$!
  trap 'kill "$PROXY_PID" 2>/dev/null || true; rm -f "$PROXY_LOG"' EXIT
  PROXY_PORT=""
  for _ in $(seq 1 20); do
    PROXY_PORT=$(sed -n 's/.*Starting to serve on 127\.0\.0\.1:\([0-9]*\).*/\1/p' "$PROXY_LOG")
    [ -n "$PROXY_PORT" ] && break
    sleep 0.5
  done
  if [ -z "$PROXY_PORT" ]; then
    echo "ERROR: kubectl proxy did not start; see $PROXY_LOG" >&2
    exit 1
  fi
  curl -s -X DELETE "http://127.0.0.1:${PROXY_PORT}/apis/jobset.x-k8s.io/v1alpha2/namespaces/$NS/jobsets" > /dev/null || true
  curl -s -X DELETE "http://127.0.0.1:${PROXY_PORT}/apis/kueue.x-k8s.io/v1beta1/namespaces/$NS/workloads" > /dev/null || true
  curl -s -X DELETE "http://127.0.0.1:${PROXY_PORT}/apis/batch/v1/namespaces/$NS/jobs" > /dev/null || true
  kill "$PROXY_PID" 2>/dev/null || true
  trap 'rm -f "$PROXY_LOG"' EXIT

  echo "==> Waiting for resources to be deleted (timeout ${WAIT_TIMEOUT_SECS}s)..."
  DEADLINE=$(( $(date +%s) + WAIT_TIMEOUT_SECS ))
  for res in jobsets workloads jobs; do
    while true; do
      count=$(kubectl get "$res" -n "$NS" --no-headers 2>/dev/null | wc -l)
      if [ "$count" -eq 0 ]; then
        echo "    -> $res deleted"
        break
      fi
      if [ "$(date +%s)" -ge "$DEADLINE" ]; then
        echo "    -> TIMED OUT with $count $res remaining. For very large runs use PURGE_NAMESPACE=1 (namespace recreate, suite-E finding #10)." >&2
        exit 1
      fi
      echo "    -> $count $res remaining... waiting 10s"
      sleep 10
    done
  done
fi

echo "==> Teardown complete. Cluster left intact with default-pool."

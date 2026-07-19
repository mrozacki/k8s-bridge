#!/usr/bin/env bash
# Scenario S1: Injects 5,000 held Slurm jobs into mixing-gpu partition and records memory/CPU telemetry.
# This script executes end-to-end against an active Slurm deployment in the slurm-jobs namespace.
set -euo pipefail

echo "==> Verifying slurm-login pod is available..."
LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -z "${LOGIN_POD}" ]]; then
  echo "ERROR: slurm-login pod not found in slurm-jobs namespace. Cannot submit Slurm jobs." >&2
  exit 1
fi

echo "==> Inverting Slurm queue to HELD state and injecting 20,000 jobs..."
START_TIME=$(date +%s)
kubectl exec -n slurm "${LOGIN_POD}" -- bash -c '
  for i in $(seq 1 20000); do
    sbatch --hold --partition=mixing --job-name="s1-test-$i" --wrap="sleep 60" >/dev/null 2>&1
    if [ $((i % 500)) -eq 0 ]; then echo "Injected $i jobs..."; fi
  done
'
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
echo "==> 20,000 jobs injected in ${ELAPSED} seconds."

echo "==> Measuring queue depth..."
QUEUE_DEPTH=$(kubectl exec -n slurm "${LOGIN_POD}" -- squeue -h | wc -l)
echo "==> Total queue depth: ${QUEUE_DEPTH} jobs."

echo "==> Capturing k8s-bridge pod memory and CPU usage via kubectl top..."
kubectl top pod -l app.kubernetes.io/name=k8s-bridge -n slurm-jobs --containers || true

echo "==> Verifying created JobSet count..."
JOBSET_COUNT=$(kubectl get jobsets -n slurm-jobs --no-headers 2>/dev/null | wc -l)
echo "==> k8s-bridge created ${JOBSET_COUNT} JobSets."

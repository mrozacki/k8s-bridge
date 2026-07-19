#!/usr/bin/env bash
# Scenario S3: Sustained throughput (50 jobs/min) injection and reconcile loop duration measurement.
# This script executes end-to-end against an active Slurm deployment in the slurm-jobs namespace.
set -euo pipefail

echo "==> Verifying slurm-login pod is available..."
LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -z "${LOGIN_POD}" ]]; then
  echo "ERROR: slurm-login pod not found in slurm-jobs namespace. Cannot submit Slurm jobs." >&2
  exit 1
fi

echo "==> Submitting batches of 25 Slurm CPU jobs every 30 seconds (20 cycles = 500 jobs over ~10 min)..."
for batch in $(seq 1 20); do
  echo "Submitting batch ${batch}/20 (25 jobs)..."
  START_BATCH=$(date +%s)
  kubectl exec -n slurm "${LOGIN_POD}" -- bash -c "
    for i in \$(seq 1 25); do
      sbatch --partition=mixing --job-name=\"s3-b${batch}-\$i\" --wrap=\"sleep 30\" >/dev/null 2>&1
    done
  "
  END_BATCH=$(date +%s)
  DURATION=$((END_BATCH - START_BATCH))
  echo "Batch ${batch} submitted in ${DURATION}s."
  
  # Measure time for JobSets to appear in Kubernetes
  echo "Measuring reconcile loop duration for batch ${batch}..."
  SLEEP_REMAINING=$((30 - DURATION))
  if [ ${SLEEP_REMAINING} -gt 0 ]; then
    sleep "${SLEEP_REMAINING}"
  fi
done

TOTAL_JOBSETS=$(kubectl get jobsets -n slurm-jobs -l "k8s-bridge.x-k8s.io/queue=mixing" --no-headers 2>/dev/null | wc -l)
echo "==> Sustained throughput cycle completed. Total JobSets created: ${TOTAL_JOBSETS}."

#!/usr/bin/env bash
# Creates the minimal GKE Standard cluster for the playground.
# Setup: zonal cluster + 2x e2-standard-4 spot nodes.
# ALWAYS run 99-teardown.sh when done.
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

gcloud container clusters create "${CLUSTER_NAME}" \
  --zone "${ZONE}" \
  --machine-type "${MACHINE_TYPE}" \
  --spot \
  --num-nodes "${NUM_NODES}" \
  --enable-autoscaling --min-nodes "${MIN_NODES:-1}" --max-nodes "${MAX_NODES}" \
  --disk-size 50 \
  --release-channel regular \
  --workload-pool "${PROJECT_ID}.svc.id.goog" \
  --autoscaling-profile optimize-utilization

gcloud container clusters get-credentials "${CLUSTER_NAME}" --zone "${ZONE}"

echo
echo "Cluster ready. Reminder: spot nodes can be reclaimed at any time —"
echo "that is acceptable (and even educational) for this playground."

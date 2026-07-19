#!/usr/bin/env bash
# Creates the GKE Standard cluster for the scale + GPU-churn run: three node
# pools (control-plane, GPU-sim/spot, churn) so a spot reclamation or a churn
# storm cannot take down the bridge/Kueue/slurmctld control loop.
#
# Scale profile: see README.md "Scale profile". ALWAYS run 99-teardown.sh at
# the end of the session — autoscaling min=0 on the two workload pools keeps
# idle resource use near zero, but the cluster and any leftover PVC disks
# remain until the cluster itself is deleted.
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

echo "==> Creating cluster ${CLUSTER_NAME} with default (control-plane) pool"
gcloud container clusters create "${CLUSTER_NAME}" \
  --zone "${ZONE}" \
  --machine-type "${CTRL_MACHINE_TYPE}" \
  --num-nodes "${CTRL_MIN_NODES}" \
  --enable-autoscaling --min-nodes "${CTRL_MIN_NODES}" --max-nodes "${CTRL_MAX_NODES}" \
  --disk-size 50 \
  --release-channel regular \
  --workload-pool "${PROJECT_ID}.svc.id.goog" \
  --autoscaling-profile optimize-utilization \
  --node-labels=pool=control-plane

echo "==> Adding GPU-sim spot pool: ${MACHINE_TYPE} x [${MIN_NODES}..${MAX_NODES}]"
gcloud container node-pools create gpu-sim-spot \
  --cluster "${CLUSTER_NAME}" --zone "${ZONE}" \
  --machine-type "${MACHINE_TYPE}" \
  --spot \
  --num-nodes "${MIN_NODES}" \
  --enable-autoscaling --min-nodes "${MIN_NODES}" --max-nodes "${MAX_NODES}" \
  --disk-size 30 \
  --node-labels=pool=gpu-sim,workload-mixing/gpu-sim=true \
  --node-taints=workload-mixing/gpu-sim=true:NoSchedule

echo "==> Adding churn pool: ${CHURN_MACHINE_TYPE} x [${CHURN_MIN_NODES}..${CHURN_MAX_NODES}]"
gcloud container node-pools create churn-pool \
  --cluster "${CLUSTER_NAME}" --zone "${ZONE}" \
  --machine-type "${CHURN_MACHINE_TYPE}" \
  --spot \
  --num-nodes "${CHURN_MIN_NODES}" \
  --enable-autoscaling --min-nodes "${CHURN_MIN_NODES}" --max-nodes "${CHURN_MAX_NODES}" \
  --disk-size 30 \
  --node-labels=pool=churn,workload-mixing/churn=true \
  --node-taints=workload-mixing/churn=true:NoSchedule

gcloud container clusters get-credentials "${CLUSTER_NAME}" --zone "${ZONE}"

echo
echo "Cluster ready with 3 pools: control-plane (on-demand, ${CTRL_MACHINE_TYPE}),"
echo "gpu-sim-spot (spot, ${MACHINE_TYPE}, 0..${MAX_NODES}), churn-pool (spot, ${CHURN_MACHINE_TYPE}, 0..${CHURN_MAX_NODES})."
echo "Both workload pools scale from zero — no cost until pods are scheduled onto them."
echo "Reminder: spot nodes can be reclaimed at any time; that is expected here."
echo "Workloads destined for gpu-sim/churn pools need matching tolerations + nodeSelector"
echo "(see manifests/ and scripts/20-scale-backlog.sh, scripts/30-churn.sh)."

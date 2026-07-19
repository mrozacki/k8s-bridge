#!/usr/bin/env bash
# Shared environment for experiment 09 (scale + simulated-GPU churn). Source
# this file, do not execute it:
#   source scripts/00-env.sh
#
# Conventions copied from experiments/01-gke-playground/scripts/00-env.sh:
# PROJECT_ID stays out of the repo, ${VAR:-default} lets a caller override
# any lever from the shell, e.g. `MAX_NODES=80 ./scripts/01-create-cluster.sh`.

export PROJECT_ID="${PROJECT_ID:?set PROJECT_ID to your GCP project, e.g. export PROJECT_ID=${PROJECT_ID:?set your GCP project id}}"
export ZONE="${ZONE:-europe-west1-b}" # zonal cluster: management fee covered by GKE free tier
export CLUSTER_NAME="${CLUSTER_NAME:-k8s-bridge-scale}"

# ---------------------------------------------------------------------------
# Node pool sizing for the "500 RUNNING slurmd pods" target.
#
# Machine type: e2-standard-8 = 8 vCPU / 32Gi per node.
# Each slurmd pod (translated JobSet member) requests:
#   cpu: 100m, memory: 256Mi, nvidia.com/gpu: 1 (simulated, see 10-simulate-gpus.sh)
#
# CPU/memory alone would pack ~80 pods/node (8000m / 100m) — far more than we
# want per node for a scheduler-stress test, and irrelevant anyway because the
# GPU-sim daemonset patches a DELIBERATELY SMALL fake nvidia.com/gpu capacity
# per node so the GPU count, not CPU, is the binding constraint (this is what
# forces the scheduler and Kueue to actually spread work across many nodes,
# which is the point of a "500 running" test — a single giant node would not
# exercise cluster-wide scheduling/informer fan-out realistically).
#
# GPU_PER_NODE simulated units per node (see 10-simulate-gpus.sh):
export GPU_PER_NODE="${GPU_PER_NODE:-8}"
# => 8 slurmd-GPU-pods/node fit the GPU gate; CPU headroom (8 * 100m = 800m of
#    8000m) and memory (8 * 256Mi = 2Gi of 32Gi) are nowhere near the limit,
#    confirming GPU count is the intended bottleneck, not CPU/memory.
#
# Nodes needed for 500 RUNNING pods: ceil(500 / 8) = 63 nodes minimum, plus
# headroom for system DaemonSets (gpu-sim patcher, kube-proxy, GKE addons) and
# for the node carrying the bridge/Kueue/slurmctld control-plane pods (those
# run on a SEPARATE small on-demand pool, see 01-create-cluster.sh — never on
# the spot GPU-sim pool, to keep control-plane pods off preemptible capacity).
# We round up to 70 nodes of headroom and set MAX_NODES a bit above that so
# autoscaling has slack during churn spikes.
export RUNNING_TARGET="${RUNNING_TARGET:-500}"
export NODES_FOR_TARGET=$(( (RUNNING_TARGET + GPU_PER_NODE - 1) / GPU_PER_NODE ))
export MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
export MIN_NODES="${MIN_NODES:-0}"   # autoscale to zero when idle -> near-zero cost between runs
export MAX_NODES="${MAX_NODES:-80}"  # NODES_FOR_TARGET (63) + ~25% headroom, rounded

# Separate tiny on-demand pool for control-plane pods (bridge, Kueue, KubeRay,
# slurmctld, the churn generator) so a spot reclamation of the big GPU-sim
# pool never takes the controllers down with it.
export CTRL_MACHINE_TYPE="${CTRL_MACHINE_TYPE:-e2-standard-4}"
export CTRL_MIN_NODES="${CTRL_MIN_NODES:-1}"
export CTRL_MAX_NODES="${CTRL_MAX_NODES:-3}"

# Dedicated tiny pool for the pod-churn generator (30-churn.sh). Kept separate
# from the 500-slurmd-pod GPU pool so churn create/delete storms cannot evict
# or starve the "real" running jobs we are trying to hold steady while we
# measure churn — see 30-churn.sh header for the full rationale.
export CHURN_MACHINE_TYPE="${CHURN_MACHINE_TYPE:-e2-standard-4}"
export CHURN_MIN_NODES="${CHURN_MIN_NODES:-0}"
export CHURN_MAX_NODES="${CHURN_MAX_NODES:-4}"

# Backlog target: 3000 PENDING (held) Slurm jobs queued behind the 500 running.
export PENDING_TARGET="${PENDING_TARGET:-3000}"

# Churn target: sustained pod create+delete rate the cluster must sustain.
export CHURN_TARGET_PODS_PER_SEC="${CHURN_TARGET_PODS_PER_SEC:-100}"

# Image ref: fill TAG from the build step before running 02-install-components
# equivalent (this experiment reuses 01-gke-playground's install script for
# the shared building blocks; only the bridge image + values differ).
export BRIDGE_IMAGE="${BRIDGE_IMAGE:-${REGISTRY:?set REGISTRY, e.g. REGION-docker.pkg.dev/PROJECT/REPO}/k8s-bridge:TAG}"

# Component versions — kept in sync with experiments/01-gke-playground/scripts/00-env.sh
# (verified 2026-07-03 against upstream release pages). Re-check before a live
# run in case a newer patch release has shipped.
export KUEUE_VERSION="${KUEUE_VERSION:-v0.18.2}"
export JOBSET_VERSION="${JOBSET_VERSION:-v0.12.0}"
export SLURM_OPERATOR_CHART_VERSION="${SLURM_OPERATOR_CHART_VERSION:-}"

# Namespaces
export BRIDGE_NAMESPACE="${BRIDGE_NAMESPACE:-slurm-jobs}"     # matches chart config.namespace convention
export CHURN_NAMESPACE="${CHURN_NAMESPACE:-churn-test}"        # isolated from bridge-managed objects

gcloud config set project "${PROJECT_ID}" >/dev/null
echo "Environment loaded: project=${PROJECT_ID} zone=${ZONE} cluster=${CLUSTER_NAME}"
echo "Node math: RUNNING_TARGET=${RUNNING_TARGET} GPU_PER_NODE=${GPU_PER_NODE} -> NODES_FOR_TARGET=${NODES_FOR_TARGET} (MAX_NODES=${MAX_NODES})"

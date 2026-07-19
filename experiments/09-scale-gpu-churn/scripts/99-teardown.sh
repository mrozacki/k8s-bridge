#!/usr/bin/env bash
# Deletes everything experiment 09 created and prints the exact gcloud
# commands to VERIFY zero billing afterwards. Mirrors
# experiments/01-gke-playground/scripts/99-teardown.sh, scoped to this
# experiment's extra objects (3 node pools, churn namespace, gpu-sim
# DaemonSet + RBAC, patched node status).
#
# TEARDOWN IS MANDATORY — run this at the end of every live session, even if
# a run was aborted partway through. Autoscaling min=0 on the workload pools
# keeps IDLE cost near zero, but it does not stop the cluster management fee,
# and node-status patches / leftover PVCs / namespaces are NOT cleaned up by
# just scaling pools down.
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

echo "==> Scaling down churn generator (if it still exists)"
kubectl -n "${CHURN_NAMESPACE}" scale deployment/churn-generator --replicas=0 2>/dev/null || true

echo "==> Deleting churn namespace (${CHURN_NAMESPACE}) — removes churn-generator Deployment + all its pods"
kubectl delete namespace "${CHURN_NAMESPACE}" --ignore-not-found --wait=true

echo "==> Deleting gpu-sim-patcher DaemonSet + RBAC (kube-system)"
kubectl delete daemonset gpu-sim-patcher -n kube-system --ignore-not-found
kubectl delete clusterrolebinding gpu-sim-patcher --ignore-not-found
kubectl delete clusterrole gpu-sim-patcher --ignore-not-found
kubectl delete serviceaccount gpu-sim-patcher -n kube-system --ignore-not-found

echo "==> Deleting bridge-managed backlog namespace objects (${BRIDGE_NAMESPACE})"
echo "    (JobSets/Workloads/held Slurm jobs are removed with the cluster below;"
echo "    this step only matters if you are tearing down objects but KEEPING"
echo "    the cluster for a follow-up run — uncomment if that's the intent)"
# kubectl delete jobsets -n "${BRIDGE_NAMESPACE}" --all
# kubectl delete workloads -n "${BRIDGE_NAMESPACE}" --all

echo "==> Deleting the entire cluster (all 3 node pools go with it: control-plane, gpu-sim-spot, churn-pool)"
gcloud container clusters delete "${CLUSTER_NAME}" --zone "${ZONE}" --quiet

# Live finding carried over from experiment 01 (2026-07-04): cluster deletion
# does NOT remove dynamically provisioned PVC disks (e.g. slurmctld's state
# volume) — delete any orphaned pvc-* disks explicitly.
#
# The cluster-name label keeps this safe in a shared project (an unscoped
# `name~^pvc-` filter would delete other engineers' volumes). A zero match can
# ALSO mean the label is absent (older provisioner, Autopilot, regional PD),
# leaving real orphans to bill silently — report that, never widen the delete.
echo "==> Checking for orphaned PVC disks"
disks=$(gcloud compute disks list \
  --filter="name~^pvc- AND labels.goog-k8s-cluster-name=${CLUSTER_NAME}" \
  --zones="${ZONE}" --format="value(name)" 2>/dev/null)
if [ -z "${disks}" ]; then
  unlabelled=$(gcloud compute disks list --filter="name~^pvc-" --zones="${ZONE}" \
    --format="value(name)" 2>/dev/null | wc -l | tr -d ' ')
  if [ "${unlabelled}" -gt 0 ]; then
    echo "WARNING: no pvc-* disk carries labels.goog-k8s-cluster-name=${CLUSTER_NAME},"
    echo "         but ${unlabelled} pvc-* disk(s) exist in ${ZONE}. They may be orphans"
    echo "         from this cluster. Inspect and delete them by hand — this script"
    echo "         will not delete unlabelled disks in a shared project."
  else
    echo "No orphaned PVC disks found."
  fi
else
  for disk in ${disks}; do
    echo "Deleting orphaned PVC disk: ${disk}"
    gcloud compute disks delete "${disk}" --zone "${ZONE}" --quiet
  done
fi

echo
echo "=============================================================="
echo "TEARDOWN COMPLETE. VERIFY ZERO BILLING with these commands:"
echo "=============================================================="
echo
echo "# 1. No clusters left (this experiment's cluster must be gone):"
echo "gcloud container clusters list"
echo
echo "# 2. No compute instances left (node pool VMs, including any strays):"
echo "gcloud compute instances list --format='table(name,zone,status)'"
echo
echo "# 3. No orphaned disks (PVCs in particular auto-provision and outlive the cluster):"
echo "gcloud compute disks list --format='table(name,zone,sizeGb)'"
echo
echo "# 4. No forwarding rules / load balancers left billing:"
echo "gcloud compute forwarding-rules list"
echo
echo "Run all four now:"
gcloud container clusters list
gcloud compute instances list --format="table(name,zone,status)"
gcloud compute disks list --format="table(name,zone,sizeGb)" 2>/dev/null || true
gcloud compute forwarding-rules list 2>/dev/null | head -5 || true

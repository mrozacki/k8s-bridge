#!/usr/bin/env bash
# Deletes the playground cluster. Run this at the END OF EVERY SESSION —
# an idle cluster still bills for its nodes.
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

gcloud container clusters delete "${CLUSTER_NAME}" --zone "${ZONE}" --quiet

# Live finding (2026-07-04): cluster deletion does NOT remove dynamically
# provisioned PVC disks (e.g. the Slurm controller's state volume) — delete
# any orphaned pvc-* disks explicitly.
#
# The cluster-name label is what keeps this safe in a shared project: an
# unscoped `name~^pvc-` filter would delete other engineers' volumes. But a
# zero match can ALSO mean the label is simply absent (older provisioner,
# Autopilot, regional PD), in which case real orphans would linger and keep
# billing silently. Report that case; never widen the delete.
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

echo "Cluster deleted. Double-check nothing else is billing:"
gcloud compute instances list --format="table(name,zone,status)"
gcloud compute disks list --format="table(name,zone,sizeGb)" 2>/dev/null || true
gcloud compute forwarding-rules list 2>/dev/null | head -3 || true

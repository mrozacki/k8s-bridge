#!/usr/bin/env bash
# Applies the Kueue queuing topology (flavors, queues, priority classes).
set -euo pipefail
cd "$(dirname "$0")/.."

# Preflight (live finding 2026-07-27, tutorial run). This script used to check
# nothing. When 01/02 bailed out — e.g. PROJECT_ID unset, which makes 00-env.sh
# fail hard — the tutorial's bring-up block still reached this line (the block
# chained its steps by newline, not by &&), so the first thing the user saw was
# `no matches for kind "ResourceFlavor"`: a missing-CRD error that reads like a
# broken manifest instead of "the two previous steps never ran". Fail with the
# actual cause instead. Deliberately does NOT source 00-env.sh — this step is
# pure kubectl and must stay runnable against any cluster, GKE or not.
if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: no reachable Kubernetes cluster in the current kubectl context." >&2
  echo "       Run 01-create-cluster.sh first (it also fetches credentials)." >&2
  exit 1
fi

if ! kubectl get crd clusterqueues.kueue.x-k8s.io >/dev/null 2>&1; then
  echo "ERROR: the Kueue CRDs are not installed in this cluster." >&2
  echo "       Run 02-install-components.sh first — without it every object in" >&2
  echo "       manifests/kueue-config.yaml fails as 'no matches for kind ...'." >&2
  exit 1
fi

# Sizing guard for the same finding's second half: the demo maps one Slurm task
# to a full CPU, so a node whose ALLOCATABLE cpu is under 1000m can never run a
# single-task job and every job sits Pending behind a quota-shaped message.
# e2-standard-4 (this playground's default, see 00-env.sh) is far above the line;
# a hand-created cluster on e2-medium (~940m allocatable) is not. Warn, do not
# fail — an operator may deliberately run a different CPU-per-task mapping.
max_alloc=$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.allocatable.cpu}{"\n"}{end}' 2>/dev/null |
  sed 's/m$//; s/^\([0-9][0-9]*\)$/\1000/' | sort -n | tail -1)
if [ -n "${max_alloc}" ] && [ "${max_alloc}" -lt 1000 ] 2>/dev/null; then
  echo "WARNING: the largest node in this cluster advertises ${max_alloc}m allocatable CPU."
  echo "         The demo requests 1000m per Slurm task, so jobs will stay Pending"
  echo "         forever. Recreate the cluster with MACHINE_TYPE=e2-standard-4 (the"
  echo "         default in 00-env.sh) or larger."
fi

kubectl apply --server-side -f manifests/kueue-config.yaml

echo
echo "Queue topology:"
kubectl get clusterqueues,localqueues,workloadpriorityclasses -A

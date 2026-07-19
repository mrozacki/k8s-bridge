#!/usr/bin/env bash
# Makes the gpu-sim-spot pool advertise a simulated nvidia.com/gpu extended
# resource, per ADR-0010 mechanism 1 ("patch a node's status with a fake
# extended resource... Scheduler, Kueue quota, and admission treat it as
# real; pods requesting it schedule normally, no device is mounted").
#
# Mechanism chosen: a self-patching DaemonSet (manifests/gpu-sim-daemonset.yaml)
# rather than a one-shot `kubectl patch node .../status` loop over the pool.
# Rationale (see the manifest's header comment for the full argument): the
# gpu-sim-spot pool autoscales 0..N, so a one-shot loop would miss every node
# created after it ran; a DaemonSet's pod is scheduled automatically onto new
# nodes as they join, so the fake capacity is applied within ~30s of any
# scale-up, no re-run required. This is exactly the "small daemonset that
# patches its own node's capacity" option the experiment brief asked us to
# evaluate and prefer.
#
# This script is idempotent: kubectl apply on the same manifest is a no-op if
# nothing changed, and the DaemonSet's own patch loop is a no-op re-apply of
# the same value every 30s.
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

MANIFEST_DIR="$(dirname "$0")/../manifests"
RENDERED="$(mktemp)"
trap 'rm -f "${RENDERED}"' EXIT

echo "==> Rendering gpu-sim-daemonset.yaml with GPU_PER_NODE=${GPU_PER_NODE}"
GPU_PER_NODE="${GPU_PER_NODE}" envsubst '${GPU_PER_NODE}' \
  < "${MANIFEST_DIR}/gpu-sim-daemonset.yaml" > "${RENDERED}"

echo "==> Applying gpu-sim-patcher DaemonSet (kube-system)"
kubectl apply -f "${RENDERED}"

echo "==> Waiting for DaemonSet rollout (this only completes once >=1 gpu-sim node exists;"
echo "    if the pool is currently scaled to zero, skip the wait and check later)"
if kubectl get nodes -l workload-mixing/gpu-sim=true --no-headers 2>/dev/null | grep -q .; then
  kubectl -n kube-system rollout status daemonset/gpu-sim-patcher --timeout=120s
else
  echo "    (no gpu-sim nodes exist yet — the DaemonSet will activate as soon as autoscaling adds one)"
fi

echo
echo "==> Verifying advertised capacity on any current gpu-sim nodes:"
kubectl get nodes -l workload-mixing/gpu-sim=true \
  -o custom-columns='NODE:.metadata.name,GPU-CAPACITY:.status.capacity.nvidia\.com/gpu,GPU-ALLOCATABLE:.status.allocatable.nvidia\.com/gpu' \
  2>/dev/null || echo "(no gpu-sim nodes currently scheduled — scale the pool up or wait for the backlog script to trigger autoscaling)"

echo
echo "Simulated GPU resource: nvidia.com/gpu (matches Slurmd.GPUResourceName default"
echo "in internal/config/config.go and ADR-0010). No real"
echo "accelerator hardware is requested or billed."

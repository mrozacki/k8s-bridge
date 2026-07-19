#!/usr/bin/env bash
# Simulates a two-level datacenter topology on the playground by labeling
# worker nodes: 2 blocks x 2 racks (one node per rack with 4 nodes).
# The labels are the ONLY thing Kueue TAS looks at — which is why a
# simulated hierarchy behaves identically to a real one.
set -euo pipefail

NODES=($(kubectl get nodes -o name | cut -d/ -f2 | sort))
if [ "${#NODES[@]}" -lt 4 ]; then
  echo "need >= 4 nodes for the 2x2 topology, found ${#NODES[@]}" >&2
  exit 1
fi

BLOCKS=(block-a block-a block-b block-b)
RACKS=(rack-1 rack-2 rack-1 rack-2)

for i in 0 1 2 3; do
  kubectl label node "${NODES[$i]}" \
    "example.com/topology-managed=true" \
    "example.com/block=${BLOCKS[$i]}" \
    "example.com/rack=${RACKS[$i]}" \
    --overwrite
done

echo
echo "Simulated topology:"
kubectl get nodes -L example.com/block,example.com/rack --no-headers \
  | awk '{printf "  %-55s %s/%s\n", $1, $6, $7}'

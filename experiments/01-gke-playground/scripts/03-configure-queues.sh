#!/usr/bin/env bash
# Applies the Kueue queuing topology (flavors, queues, priority classes).
set -euo pipefail
cd "$(dirname "$0")/.."

kubectl apply --server-side -f manifests/kueue-config.yaml

echo
echo "Queue topology:"
kubectl get clusterqueues,localqueues,workloadpriorityclasses -A

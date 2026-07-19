#!/bin/bash
set -ex
export EVID="$HOME/k8s-bridge-testrun/TC-E3"
mkdir -p "$EVID"

# baseline
kubectl get pods -n slurm-jobs -l app.kubernetes.io/name=k8s-bridge -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' > "$EVID/baseline-restarts.txt"

# Run s2-churn.sh
experiments/11-scale-s1-s5/scripts/s2-churn.sh | tee "$EVID/churn-log.txt"

# Collect metrics
kubectl get nodes --no-headers | wc -l > "$EVID/max-nodes.txt"

# wait a while to let autoscaler scale down (up to 10 minutes)
for i in $(seq 1 10); do
   kubectl get nodes | awk '/churn-pool/ {print $1}' | wc -l >> "$EVID/node-counts.txt"
   sleep 60
done

kubectl -n slurm-jobs logs -l app.kubernetes.io/name=k8s-bridge --tail=5000 | grep -ciE 'deadlock|OOM' > "$EVID/errors.txt" || true

kubectl get pods -n slurm-jobs -l app.kubernetes.io/name=k8s-bridge -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' > "$EVID/end-restarts.txt"

# Teardown
kubectl delete deploy churn-generator -n slurm-jobs || true

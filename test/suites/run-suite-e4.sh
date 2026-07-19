#!/bin/bash
set -ex
export EVID="$HOME/k8s-bridge-testrun/TC-E4"
mkdir -p "$EVID"

LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

experiments/11-scale-s1-s5/scripts/s3-throughput.sh | tee "$EVID/throughput.txt"

kubectl -n slurm-jobs logs -l app.kubernetes.io/name=k8s-bridge --tail=5000 | grep -ciE 'tick error' > "$EVID/tick-errors.txt" || true

experiments/11-scale-s1-s5/scripts/99-teardown.sh || true
kubectl exec -n slurm "$LOGIN_POD" -- scancel --state=PENDING -u root || true
kubectl exec -n slurm "$LOGIN_POD" -- scancel --state=RUNNING -u root || true

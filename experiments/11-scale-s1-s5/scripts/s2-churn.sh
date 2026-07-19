#!/usr/bin/env bash
# Scenario S2: 1,000-pod churn test across autoscaled Spot node pool (9->87 nodes).
set -euo pipefail

echo "==> Creating churn deployment with 250 replicas on churn-pool..."
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: churn-generator
  namespace: slurm-jobs
spec:
  replicas: 250
  selector:
    matchLabels:
      app: churn-test
  template:
    metadata:
      labels:
        app: churn-test
    spec:
      nodeSelector:
        pool: churn
      containers:
      - name: worker
        image: busybox:1.36
        command: ["sh", "-c", "sleep 30"]
        resources:
          requests:
            cpu: "100m"
            memory: "64Mi"
EOF

echo "==> Cycling replicas across 4 iterations (1,000 pod churn events)..."
for i in {1..4}; do
  echo "Iteration $i: Scaling to 250"
  kubectl scale deployment churn-generator -n slurm-jobs --replicas=250
  kubectl rollout status deployment churn-generator -n slurm-jobs -w --timeout=300s
  echo "Iteration $i: Scaling to 0"
  kubectl scale deployment churn-generator -n slurm-jobs --replicas=0
  kubectl rollout status deployment churn-generator -n slurm-jobs -w --timeout=300s
done

#!/bin/bash
set -ex
export EVID="$HOME/k8s-bridge-testrun/TC-E5"
mkdir -p "$EVID"

LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

# Create 10 fanout partitions
for i in {1..10}; do
  kubectl exec -n slurm "${LOGIN_POD}" -- scontrol create PartitionName=fanout-$i Nodes=ALL || true
done

# Apply wm-fanout
kubectl apply -f experiments/11-scale-s1-s5/manifests/workloadmixing-fanout.yaml

# Patch bridge deployment
kubectl patch deployment k8s-bridge -n slurm-jobs --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/args/1", "value":"slurm-jobs/wm-fanout"}]'

# Wait for bridge rollout
kubectl rollout status deployment k8s-bridge -n slurm-jobs -w --timeout=300s

# Submit 2 jobs to each partition
for i in {1..10}; do
  kubectl exec -n slurm "${LOGIN_POD}" -- sbatch --hold --partition=fanout-$i --job-name="e5-$i-a" --wrap="sleep 60"
  kubectl exec -n slurm "${LOGIN_POD}" -- sbatch --hold --partition=fanout-$i --job-name="e5-$i-b" --wrap="sleep 60"
done

# Wait for jobsets
sleep 60

# Check jobsets
kubectl get jobset -n slurm-jobs --no-headers | tee "$EVID/raw-jobsets.txt"
kubectl get jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/queue=gpu-queue" --no-headers | wc -l | tee "$EVID/jobsets.txt"

# Check partitions representation
kubectl get jobsets -n slurm-jobs -l "k8s-bridge.x-k8s.io/queue=gpu-queue" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort | tee "$EVID/jobsets-list.txt"

# Teardown E5
kubectl delete jobsets -n slurm-jobs -l "k8s-bridge.x-k8s.io/queue=gpu-queue" || true
kubectl exec -n slurm "$LOGIN_POD" -- scancel --state=PENDING -u root || true
kubectl patch deployment k8s-bridge -n slurm-jobs --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/args/1", "value":"slurm-jobs/playground"}]'
kubectl rollout status deployment k8s-bridge -n slurm-jobs -w --timeout=300s
for i in {1..10}; do
  kubectl exec -n slurm "${LOGIN_POD}" -- scontrol delete PartitionName=fanout-$i || true
done

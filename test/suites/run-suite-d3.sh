#!/bin/bash
set -ex

LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

echo "=== TC-D3 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-D3"
mkdir -p "$EVID"

kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --partition=mixing --wrap="srun sleep 300"' | tee "$EVID/submit.txt"
JOBID=$(grep -oE '[0-9]+' "$EVID/submit.txt" | tail -1)

# Ensure job is running
echo "Waiting for actual slurmd pod to exist..."
for i in $(seq 1 20); do
  POD=$(kubectl get pods -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "$POD" ]]; then break; fi
  sleep 2
done

echo "Killing pod $POD..."
kubectl delete pod "$POD" -n slurm-jobs --wait=false | tee "$EVID/kill.txt"

echo "Observing Slurm reaction for 120s..."
for i in $(seq 1 15); do
  kubectl exec -n slurm "$LOGIN_POD" -- sinfo -N -o '%N %T' | tee -a "$EVID/sinfo.txt"
  sleep 10
done

kubectl exec -n slurm "$LOGIN_POD" -- squeue -j "$JOBID" -h -o '%T %r' | grep -v 'RUNNING' | tee "$EVID/state.txt" || true
ST=$(cat "$EVID/state.txt")
if [[ "$ST" == *"PENDING"* ]]; then
  echo "TC-D3 passed. Job requeued."
else
  echo "TC-D3 failed. Job is not pending ($ST)."
  exit 1
fi

kubectl exec -n slurm "$LOGIN_POD" -- scancel "$JOBID" 2>/dev/null || true
kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --ignore-not-found || true

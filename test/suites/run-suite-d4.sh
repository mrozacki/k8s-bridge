#!/bin/bash
set -ex

LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')
WM_NAME=$(kubectl get workloadmixing -n slurm-jobs -o jsonpath='{.items[0].metadata.name}')

echo "=== TC-D4 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-D4"
mkdir -p "$EVID"

kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --partition=mixing --wrap="srun sleep 180"' | tee "$EVID/submit.txt"
JOBID=$(grep -oE '[0-9]+' "$EVID/submit.txt" | tail -1)
kubectl exec -n slurm "$LOGIN_POD" -- scontrol release "$JOBID"

# Ensure job is running
echo "Waiting for job to be Running..."
for i in $(seq 1 20); do
  R=$(kubectl exec -n slurm "$LOGIN_POD" -- squeue -j "$JOBID" -h -o '%T' 2>/dev/null | tr -d ' ')
  if [[ "$R" == "RUNNING" ]]; then break; fi
  sleep 5
done

RESTARTS_BEFORE=$(kubectl -n slurm-jobs get pod -l app.kubernetes.io/name=k8s-bridge -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}')

# Patch the CR
echo "Patching workload mixing CR..."
kubectl patch workloadmixing "$WM_NAME" -n slurm-jobs --type=json -p='[{"op":"add","path":"/spec/partitionMappings/-","value":{"partitionName":"mixing-extra","workloadPriorityClass":"normal-priority"}}]' | tee "$EVID/patch.txt"

# Wait a bit
sleep 15

R=$(kubectl exec -n slurm "$LOGIN_POD" -- squeue -j "$JOBID" -h -o '%T' 2>/dev/null | tr -d ' ')
RESTARTS_AFTER=$(kubectl -n slurm-jobs get pod -l app.kubernetes.io/name=k8s-bridge -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}')

if [[ "$R" == "RUNNING" && "$RESTARTS_BEFORE" == "$RESTARTS_AFTER" ]]; then
  echo "TC-D4 passed. Job is still RUNNING and bridge didn't restart."
else
  echo "TC-D4 failed. Job state: $R. Restarts: $RESTARTS_BEFORE -> $RESTARTS_AFTER"
  exit 1
fi

# Removing the extra partition mapping to revert
PATCH=$(kubectl get workloadmixing "$WM_NAME" -n slurm-jobs -o json | jq -c '
  {spec:{partitionMappings: [ .spec.partitionMappings[] | select(.partitionName != "mixing-extra") ]}}
  ')
kubectl patch workloadmixing "$WM_NAME" -n slurm-jobs --type=merge -p "$PATCH"

kubectl exec -n slurm "$LOGIN_POD" -- scancel "$JOBID" 2>/dev/null || true
kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --ignore-not-found || true

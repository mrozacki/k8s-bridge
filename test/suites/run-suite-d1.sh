#!/bin/bash
set -ex

LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

echo "=== TC-D1 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-D1"
mkdir -p "$EVID"

# Submit 3 held jobs
echo "Submitting 3 jobs to mixing..."
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --wrap="srun sleep 120"' | tee -a "$EVID/submit.txt"
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --wrap="srun sleep 120"' | tee -a "$EVID/submit.txt"
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --wrap="srun sleep 120"' | tee -a "$EVID/submit.txt"

grep -oE '[0-9]+' "$EVID/submit.txt" > "$EVID/ids.txt"
LAST_JOBID=$(tail -1 "$EVID/ids.txt")

# Wait for JobSet to appear
echo "Waiting for JobSet to appear for $LAST_JOBID..."
for i in $(seq 1 12); do kubectl get jobset -n slurm-jobs -o name | grep -q "$LAST_JOBID" && break; sleep 5; done

# Delete the bridge pod
echo "Deleting bridge pod..."
kubectl -n slurm-jobs delete pod -l app.kubernetes.io/name=k8s-bridge --wait=false | tee "$EVID/kill.txt"
kubectl -n slurm-jobs rollout status deploy -l app.kubernetes.io/name=k8s-bridge --timeout=120s | tee "$EVID/restart.txt"

echo "Waiting 90s for reconciliation..."
sleep 90

kubectl get jobset -n slurm-jobs > "$EVID/jobsets.txt"
cat "$EVID/jobsets.txt"

for JOBID in $(cat "$EVID/ids.txt"); do
  COUNT=$(kubectl get jobset -n slurm-jobs -o name | grep "$JOBID" | wc -l)
  echo "Job $JOBID has $COUNT JobSets."
  if [ "$COUNT" -ne 1 ]; then
    echo "FAIL: Job $JOBID has $COUNT JobSets!"
    exit 1
  fi
done

# Teardown D1
for JOBID in $(cat "$EVID/ids.txt"); do
  kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --ignore-not-found || true
  kubectl exec -n slurm "$LOGIN_POD" -- scancel "$JOBID" 2>/dev/null || true
done
echo "TC-D1 passed."

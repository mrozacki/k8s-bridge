#!/bin/bash
set -ex

LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

echo "=== TC-D2 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-D2"
mkdir -p "$EVID"

kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --wrap="srun sleep 120"' | tee "$EVID/submit.txt"
JOBID=$(grep -oE '[0-9]+' "$EVID/submit.txt" | tail -1)

# Wait for JobSet to exist
for i in $(seq 1 12); do kubectl get job -n slurm-jobs -l "jobset.sigs.k8s.io/jobset-name" | grep -q "$JOBID" && break; sleep 5; done
# Wait for the Kube Job itself to be created by the JobSet
# The job label would be "jobset.sigs.k8s.io/jobset-name=slurm-job-$JOBID"
kubectl delete job -n slurm-jobs -l "jobset.sigs.k8s.io/jobset-name=slurm-job-$JOBID" --wait=false | tee "$EVID/kill-job.txt"

echo "Polling slurm queue for up to 180s..."
for i in $(seq 1 36); do
  R=$(kubectl exec -n slurm "$LOGIN_POD" -- squeue -j "$JOBID" -h -o '%T' 2>/dev/null | tr -d ' ')
  echo "$i:$R"
  if [ -z "$R" ]; then break; fi
  sleep 5
done | tee "$EVID/poll.txt"

kubectl exec -n slurm "$LOGIN_POD" -- sacct -j "$JOBID" -n -o State,Reason | tee "$EVID/final.txt"

FINAL_STATE=$(cat "$EVID/final.txt" | awk '{print $1}')
if [[ "$FINAL_STATE" == "FAILED" || "$FINAL_STATE" == "CANCELLED" || -z "$FINAL_STATE" ]]; then
  echo "TC-D2 passed. Job became FAILED/CANCELLED/gone."
else
  echo "TC-D2 failed! Job is $FINAL_STATE after 180s."
  exit 1
fi

# Teardown D2
kubectl delete jobset -n slurm-jobs -l "jobset.sigs.k8s.io/jobset-name=slurm-job-$JOBID" --ignore-not-found || true
kubectl exec -n slurm "$LOGIN_POD" -- scancel "$JOBID" 2>/dev/null || true

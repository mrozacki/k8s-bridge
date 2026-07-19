#!/bin/bash
set -eo pipefail
EVID="$HOME/k8s-bridge-testrun/TC-C2"
mkdir -p "$EVID"

LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n slurm "$LOGIN_POD" -- scancel -u root

echo "Submitting 4 filler jobs (2 CPUs each) to team-b (via mixing partition)..."
for i in {1..4}; do
  kubectl exec -n slurm "$LOGIN_POD" -- sbatch -p mixing -c 2 --wrap="sleep 600"
done

echo "Waiting for team-b to admit 4 jobs (8 CPUs total), borrowing from team-a..."
sleep 10
TIMEOUT=180
ELAPSED=0
BORROW_OBSERVED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
  usage=$(kubectl get clusterqueue team-b -o jsonpath='{.status.flavorsUsage[0].resources[0].borrowed}' 2>/dev/null || echo "0")
  if [ "$usage" != "0" ] && [ "$usage" != "" ]; then
    echo "Borrowing observed: team-b borrowed $usage CPUs"
    kubectl get clusterqueue -o custom-columns=NAME:.metadata.name,BORROW:.status.flavorsUsage | tee "$EVID/borrow.txt"
    BORROW_OBSERVED=1
    break
  fi
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done

if [ "$BORROW_OBSERVED" -eq 0 ]; then
  echo "FAIL: No borrowing observed for team-b after $TIMEOUT seconds."
  kubectl get clusterqueue -o custom-columns=NAME:.metadata.name,USAGES:.status.flavorsUsage
  exit 1
fi

echo "Submitting high-priority jobs to team-a (via mixing-high partition) to force reclaim..."
kubectl exec -n slurm "$LOGIN_POD" -- sbatch -p mixing-high -N 1 -c 6 --wrap="sleep 60"

echo "Waiting for reclamation events..."
sleep 10
ELAPSED=0
RECLAIM_OBSERVED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
  # Check events for preemption
  reclaim_msg=$(kubectl get events -n slurm-jobs --field-selector reason=Preempted | grep -i "reclamation within the cohort" || true)
  if [ -n "$reclaim_msg" ]; then
    echo "Reclamation observed!"
    kubectl get events -n slurm-jobs --field-selector reason=Preempted > "$EVID/reclaim.txt"
    RECLAIM_OBSERVED=1
    break
  fi
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done

if [ "$RECLAIM_OBSERVED" -eq 0 ]; then
  echo "FAIL: No reclamation events observed after $TIMEOUT seconds."
  kubectl get events -n slurm-jobs
  kubectl exec -n slurm "$LOGIN_POD" -- scontrol show job
  exit 1
fi

kubectl exec -n slurm "$LOGIN_POD" -- scancel -u root

echo "All assertions passed!"

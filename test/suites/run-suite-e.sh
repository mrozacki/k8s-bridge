#!/bin/bash
# Suite E orchestrator: runs TC-E1..TC-E6 in order, waiting for the cluster
# to drain between test cases. Logs land in $HOME/k8s-bridge-testrun/.
set -ex
set -o pipefail

LOGDIR="$HOME/k8s-bridge-testrun"
mkdir -p "$LOGDIR"

wait_for_jobsets_clear() {
  echo "Waiting for slurm-jobs jobsets to clear..."
  while true; do
    COUNT=$(kubectl get jobsets -n slurm-jobs --no-headers 2>/dev/null | wc -l || echo 0)
    if [ "$COUNT" -eq 0 ]; then
      break
    fi
    echo "Still $COUNT jobsets remaining... waiting 30s."
    sleep 30
  done
  echo "Jobsets cleared."

  # Wait for queues to settle
  sleep 30
}

echo "=== RUNNING TC-E1 ==="
./run-suite-e1.sh | tee "$LOGDIR/run-e1.log"
wait_for_jobsets_clear

echo "=== RUNNING TC-E2 ==="
./run-suite-e2.sh | tee "$LOGDIR/run-e2.log"
echo "E2 complete. Waiting for jobsets to clear..."
wait_for_jobsets_clear

echo "=== RUNNING TC-E3 ==="
./run-suite-e3.sh | tee "$LOGDIR/run-e3.log"
wait_for_jobsets_clear

echo "=== RUNNING TC-E4 ==="
./run-suite-e4.sh | tee "$LOGDIR/run-e4.log"
wait_for_jobsets_clear

echo "=== RUNNING TC-E5 ==="
./run-suite-e5.sh | tee "$LOGDIR/run-e5.log"
wait_for_jobsets_clear

echo "=== RUNNING TC-E6 ==="
./run-suite-e6.sh | tee "$LOGDIR/run-e6.log"

echo "SUITE E COMPLETED."

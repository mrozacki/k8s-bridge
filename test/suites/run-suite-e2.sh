#!/bin/bash
set -ex

export EVID_BASE="$HOME/k8s-bridge-testrun/suite-E2"
mkdir -p "$EVID_BASE"
LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

kubectl top pod -n slurm-jobs -l app.kubernetes.io/name=k8s-bridge --containers | tee "$EVID_BASE/mem-baseline.txt"

# 1. Start injecting 20,000 jobs.
echo "Injecting 20,000 jobs..."
START_TIME=$(date +%s)
kubectl exec -n slurm "${LOGIN_POD}" -- bash -c '
  for i in $(seq 1 20000); do
    sbatch --hold --partition=mixing --job-name="e2-test-$i" --wrap="sleep 60" >/dev/null 2>&1
    if [ $((i % 1000)) -eq 0 ]; then echo "Injected $i jobs..."; fi
  done
'
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
echo "==> 20,000 jobs injected in ${ELAPSED} seconds." > "$EVID_BASE/s1.txt"

echo "Started backlogging. We will sample every 60s for 20 min."
SAMPLER="$EVID_BASE/sample_e2.sh"
cat << 'INNSCRIPT' > "$SAMPLER"
#!/bin/bash
EVID_BASE=$1
for i in $(seq 1 20); do
  kubectl top pod -n slurm-jobs -l app.kubernetes.io/name=k8s-bridge --containers >> "$EVID_BASE/timeseries.txt" || true
  kubectl get jobset -n slurm-jobs --no-headers | wc -l >> "$EVID_BASE/timeseries.txt" || true
  kubectl get --raw='/readyz' >> "$EVID_BASE/timeseries.txt" || true
  echo "" >> "$EVID_BASE/timeseries.txt"
  sleep 60
done
INNSCRIPT
chmod +x "$SAMPLER"
"$SAMPLER" "$EVID_BASE" &
SAMPLE_PID=$!

### TC-D5 Integration
export EVID="$HOME/k8s-bridge-testrun/TC-D5"
mkdir -p "$EVID"

# Run a couple of REAL jobs first
kubectl exec -n slurm "$LOGIN_POD" -- sbatch -p mixing --wrap="sleep 3600"
kubectl exec -n slurm "$LOGIN_POD" -- sbatch -p mixing --wrap="sleep 3600"
sleep 60 # Wait for them to get nodes

# Drain worker node. Wait up to 5 minutes for a running node.
NODE=""
for i in {1..30}; do
  NODE=$(kubectl get pods -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id" --field-selector=status.phase=Running -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | head -n 1)
  if [ -n "$NODE" ]; then break; fi
  echo "Waiting for a running node to drain..."
  sleep 10
done
if [ -n "$NODE" ]; then
  kubectl cordon "$NODE" | tee "$EVID/cordon.txt"
  kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30 --timeout=120s 2>&1 | tee "$EVID/drain.txt" || true

  for i in $(seq 1 30); do
    kubectl exec -n slurm "$LOGIN_POD" -- sinfo -o '%T %D' | tee -a "$EVID/sinfo.txt"
    sleep 10
  done
  kubectl uncordon "$NODE" || true
else
  echo "TC-D5 skipped: no slurmd pod nodes found!"
fi

# Wait for sampling to finish
wait $SAMPLE_PID

# Check errors
kubectl -n slurm-jobs logs -l app.kubernetes.io/name=k8s-bridge --tail=5000 | grep -ciE 'error|panic|422|OOM' | tee "$EVID_BASE/errors.txt" || true

echo "Teardown..."
experiments/11-scale-s1-s5/scripts/99-teardown.sh || true
kubectl exec -n slurm "$LOGIN_POD" -- scancel --state=PENDING -u root || true
kubectl exec -n slurm "$LOGIN_POD" -- scancel --state=RUNNING -u root || true
echo "Suite E2/D5 finished."

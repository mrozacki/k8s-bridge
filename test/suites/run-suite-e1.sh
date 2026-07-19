#!/bin/bash
set -ex

export EVID_BASE="$HOME/k8s-bridge-testrun/suite-E"
mkdir -p "$EVID_BASE"
LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

kubectl top pod -n slurm-jobs -l app.kubernetes.io/name=k8s-bridge --containers | tee "$EVID_BASE/mem-baseline.txt"

# 1. Start injecting jobs. This runs synchronously and takes a while.
echo "Running s1-backlog.sh..."
experiments/11-scale-s1-s5/scripts/s1-backlog.sh | tee "$EVID_BASE/s1.txt"

echo "Started backlogging. In the background we will sample every 60s for 15 min."
SAMPLER="$EVID_BASE/sample_e1.sh"
cat << 'INNSCRIPT' > "$SAMPLER"
#!/bin/bash
EVID_BASE=$1
for i in $(seq 1 15); do
  kubectl top pod -n slurm-jobs -l app.kubernetes.io/name=k8s-bridge --containers >> "$EVID_BASE/s1-timeseries.txt" || true
  kubectl get jobset -n slurm-jobs --no-headers | wc -l >> "$EVID_BASE/s1-timeseries.txt" || true
  sleep 60
done
INNSCRIPT
chmod +x "$SAMPLER"
"$SAMPLER" "$EVID_BASE" &
SAMPLE_PID=$!

### TC-D5 Integration
# "Run this DURING TC-E1 (5k backlog) so there is real load. Simulate a spot preemption by cordoning+draining one worker node."
export EVID="$HOME/k8s-bridge-testrun/TC-D5"
mkdir -p "$EVID"

# To do D5, we need a worker node running slurmd pods.
# Let's run a couple of REAL jobs first.
kubectl exec -n slurm "$LOGIN_POD" -- sbatch -p mixing --wrap="sleep 3600"
kubectl exec -n slurm "$LOGIN_POD" -- sbatch -p mixing --wrap="sleep 3600"
sleep 20 # Wait for them to get nodes

NODE=$(kubectl get pods -n slurm-jobs -l app=slurmd -o jsonpath='{.items[0].spec.nodeName}')
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
kubectl -n slurm-jobs logs -l app.kubernetes.io/name=k8s-bridge --tail=5000 | grep -ciE 'error|panic|422|OOM' | tee "$EVID_BASE/s1-errors.txt" || true

echo "Teardown..."
experiments/11-scale-s1-s5/scripts/99-teardown.sh || true
kubectl exec -n slurm "$LOGIN_POD" -- scancel --state=PENDING -u $(whoami) || true
kubectl exec -n slurm "$LOGIN_POD" -- scancel --state=RUNNING -u $(whoami) || true
echo "Suite E1/D5 finished."

#!/bin/bash
set -x

export PATH="$HOME/bin:$PATH"
LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

echo "=== TC-B1 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-B1"; mkdir -p "$EVID"
cat <<'EOF' | kubectl apply -f - 2>&1 | tee "$EVID/apply.txt"
apiVersion: k8s-bridge.x-k8s.io/v1alpha1
kind: WorkloadMixing
metadata:
  name: wm-invalid-b1
  namespace: slurm-jobs
spec:
  slurmRestURL: "http://slurm-restapi.slurm.svc.cluster.local:6820"
  allowInsecureHTTP: true
EOF
kubectl get workloadmixing wm-invalid-b1 -n slurm-jobs 2>&1 | tee "$EVID/get.txt"
kubectl delete workloadmixing wm-invalid-b1 -n slurm-jobs --ignore-not-found

echo "=== TC-B2 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-B2"; mkdir -p "$EVID"
kubectl apply -f experiments/11-scale-s1-s5/manifests/workloadmixing-fanout.yaml 2>&1 | tee "$EVID/apply.txt"
kubectl get workloadmixing wm-fanout -n slurm-jobs -o jsonpath='{.spec.partitionMappings[*].partitionName}' | tee "$EVID/mappings.txt"
kubectl -n slurm-jobs get deploy -l app.kubernetes.io/name=k8s-bridge -o jsonpath='{.items[0].spec.template.spec.containers[0].args}' | tee "$EVID/args.txt"
kubectl delete workloadmixing wm-fanout -n slurm-jobs --ignore-not-found

echo "=== TC-B3 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-B3"; mkdir -p "$EVID"
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --ntasks=2 --cpus-per-task=1 --wrap="srun sleep 120"' | tee "$EVID/submit.txt"
JOBID=$(grep -oE '[0-9]+' "$EVID/submit.txt" | tail -1); echo "JOBID=$JOBID" | tee "$EVID/jobid.txt"
for i in $(seq 1 12); do kubectl get jobset -n slurm-jobs -o name | grep -q "$JOBID" && break; sleep 5; done
kubectl get jobset -n slurm-jobs | tee "$EVID/jobsets.txt"
echo "Wait for JobSet pods..."
for i in $(seq 1 12); do POD=$(kubectl get pod -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null); [ -n "$POD" ] && break; sleep 5; done
kubectl wait --for=condition=Ready pod -n slurm-jobs --selector="k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --timeout=180s 2>&1 | tee "$EVID/wait.txt" || true
kubectl exec -n slurm "$LOGIN_POD" -- sinfo -N | tee "$EVID/sinfo.txt"
kubectl exec -n slurm "$LOGIN_POD" -- scontrol release "$JOBID"
for i in $(seq 1 24); do S=$(kubectl exec -n slurm "$LOGIN_POD" -- sacct -j "$JOBID" -n -o State 2>/dev/null | head -1 | tr -d ' '); echo "$S"; echo "$S" | grep -q COMPLETED && break; sleep 5; done | tee "$EVID/state.txt"
kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --ignore-not-found
kubectl exec -n slurm "$LOGIN_POD" -- scancel "$JOBID" 2>/dev/null || true

echo "=== TC-B4 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-B4"; mkdir -p "$EVID"
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --ntasks=1 --cpus-per-task=1 --wrap="srun sleep 60"' | tee "$EVID/submit.txt"
JOBID=$(grep -oE '[0-9]+' "$EVID/submit.txt" | tail -1); echo "JOBID=$JOBID" | tee "$EVID/jobid.txt"
for i in $(seq 1 12); do kubectl get jobset -n slurm-jobs -o name | grep -q "$JOBID" && break; sleep 5; done
echo "Wait for JobSet pods..."
for i in $(seq 1 12); do POD=$(kubectl get pod -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null); [ -n "$POD" ] && break; sleep 5; done
kubectl wait --for=condition=Ready pod -n slurm-jobs --selector="k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --timeout=180s 2>&1 || true
POD=$(kubectl get pods -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" -o jsonpath='{.items[0].metadata.name}')
kubectl get pod "$POD" -n slurm-jobs -o jsonpath='{.spec.containers[*].name}' | tee "$EVID/containers.txt" || true
kubectl get pod "$POD" -n slurm-jobs -o jsonpath='{.spec.volumes[*].name}' | tee "$EVID/volumes.txt" || true
kubectl exec -n slurm "$LOGIN_POD" -- scontrol release "$JOBID"
for i in $(seq 1 24); do kubectl get pods -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --no-headers 2>/dev/null | wc -l | grep -qx 0 && break; sleep 5; done
kubectl get pods -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --no-headers 2>/dev/null | wc -l | tee "$EVID/leftover.txt"
kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --ignore-not-found
kubectl exec -n slurm "$LOGIN_POD" -- scancel "$JOBID" 2>/dev/null || true

echo "=== TC-B5 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-B5"; mkdir -p "$EVID"
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --ntasks=1 --cpus-per-task=1 --wrap="srun sleep 120"' | tee "$EVID/submitA.txt"
IDA=$(grep -oE '[0-9]+' "$EVID/submitA.txt" | tail -1)
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --ntasks=1 --cpus-per-task=1 --wrap="srun sleep 120"' | tee "$EVID/submitB.txt"
IDB=$(grep -oE '[0-9]+' "$EVID/submitB.txt" | tail -1)
echo "Wait for JobSet pods..."
for i in $(seq 1 12); do POD=$(kubectl get pod -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$IDA" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null); [ -n "$POD" ] && break; sleep 5; done
kubectl wait --for=condition=Ready pod -n slurm-jobs --selector="k8s-bridge.x-k8s.io/slurm-job-id=$IDA" --timeout=180s 2>&1 || true
echo "Wait for JobSet pods..."
for i in $(seq 1 12); do POD=$(kubectl get pod -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$IDB" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null); [ -n "$POD" ] && break; sleep 5; done
kubectl wait --for=condition=Ready pod -n slurm-jobs --selector="k8s-bridge.x-k8s.io/slurm-job-id=$IDB" --timeout=180s 2>&1 || true
kubectl exec -n slurm "$LOGIN_POD" -- sinfo -N -o '%N %f' | tee "$EVID/features.txt"
kubectl exec -n slurm "$LOGIN_POD" -- scontrol release "$IDA" "$IDB"
kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$IDA" --ignore-not-found
kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$IDB" --ignore-not-found
kubectl exec -n slurm "$LOGIN_POD" -- scancel "$IDA" "$IDB" 2>/dev/null || true

echo "=== TC-B6 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-B6"; mkdir -p "$EVID"
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --exclusive --wrap="srun sleep 60"' | tee "$EVID/submit.txt"
JOBID=$(grep -oE '[0-9]+' "$EVID/submit.txt" | tail -1)
for i in $(seq 1 12); do kubectl get jobset -n slurm-jobs -o name | grep -q "$JOBID" && break; sleep 5; done
POD=$(kubectl get pods -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" -o jsonpath='{.items[0].metadata.name}' || true)
kubectl get pod "$POD" -n slurm-jobs -o jsonpath='{.spec.containers[0].resources.requests.cpu}{"\n"}' | tee "$EVID/cpu.txt" || true
kubectl get pod "$POD" -n slurm-jobs -o jsonpath='{.spec.affinity}{"\n"}' | tee "$EVID/affinity.txt" || true
kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$JOBID" --ignore-not-found
kubectl exec -n slurm "$LOGIN_POD" -- scancel "$JOBID" 2>/dev/null || true

echo "=== TC-B7 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-B7"; mkdir -p "$EVID"
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'scancel --state=PENDING -u $(whoami)' 2>/dev/null || true
echo "Waiting 30s..."
sleep 30
kubectl -n slurm-jobs logs -l app.kubernetes.io/name=k8s-bridge --tail=200 > "$EVID/logs.txt"

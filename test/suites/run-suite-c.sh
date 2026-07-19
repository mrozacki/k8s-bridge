#!/bin/bash
set -ex

LOGIN_POD=$(kubectl get pods -n slurm -l app.kubernetes.io/component=login -o jsonpath='{.items[0].metadata.name}')

echo "=== TC-C1 ==="
export EVID="$HOME/k8s-bridge-testrun/TC-C1"
mkdir -p "$EVID"

# 2. Record baseline quota
kubectl get clusterqueue -o wide | tee "$EVID/cq-before.txt"

# Shrinking ClusterQueue to 4 CPU so it's easy to fill.
kubectl patch clusterqueue main-queue --type='json' -p='[{"op": "replace", "path": "/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota", "value": "4"}]'
sleep 2

# 3. Submit low-priority job sized to consume all quota
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --ntasks=4 --cpus-per-task=1 --wrap="srun sleep 60"' | tee "$EVID/submit-low.txt"
LOW_JOBID=$(grep -oE '[0-9]+' "$EVID/submit-low.txt" | tail -1)
echo "LOW_JOBID=$LOW_JOBID" | tee "$EVID/low_jobid.txt"

# Wait for JobSet to exist before releasing
for i in $(seq 1 12); do kubectl get jobset -n slurm-jobs -o name | grep -q "$LOW_JOBID" && break; sleep 5; done
kubectl exec -n slurm "$LOGIN_POD" -- scontrol release "$LOW_JOBID"

echo "Waiting for low-priority pods to start..."
for i in $(seq 1 12); do kubectl get pods -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$LOW_JOBID" --field-selector status.phase=Running | grep -q 'Running' && break; sleep 5; done

# 4. Submit high-priority job of the same size. Do NOT hold it.
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --partition=mixing-high --ntasks=4 --cpus-per-task=1 --wrap="srun sleep 60"' | tee "$EVID/submit-high.txt"
HIGH_JOBID=$(grep -oE '[0-9]+' "$EVID/submit-high.txt" | tail -1)
echo "HIGH_JOBID=$HIGH_JOBID" | tee "$EVID/high_jobid.txt"

# 5. Observe preemption on both sides
sleep 15
kubectl get workloads -n slurm-jobs | tee "$EVID/workloads.txt" || true
kubectl get events -n slurm-jobs --field-selector reason=Preempted | tee "$EVID/events.txt" || true
kubectl exec -n slurm "$LOGIN_POD" -- squeue -j "$LOW_JOBID" -h -o '%T %r' | tee "$EVID/low-state.txt" || true

# Restore clusterqueue
kubectl patch clusterqueue main-queue --type='json' -p='[{"op": "replace", "path": "/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota", "value": "32"}]'

# Teardown
kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$LOW_JOBID" --ignore-not-found
kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$HIGH_JOBID" --ignore-not-found
kubectl exec -n slurm "$LOGIN_POD" -- scancel "$LOW_JOBID" "$HIGH_JOBID" 2>/dev/null || true

echo "=== TC-C2 (Cohort borrowing then within-cohort reclaim) ==="
export EVID="$HOME/k8s-bridge-testrun/TC-C2"
mkdir -p "$EVID"

# TC-C2 routes Slurm jobs to two different LocalQueues that share a Kueue
# cohort, so borrowing and within-cohort reclamation can be exercised. Per the
# team decision, that routing is applied as a TEST-ONLY runtime patch of the
# WorkloadMixing CR (hot-reloaded) and reverted on exit — it is deliberately
# NOT baked into the Helm chart's values.yaml, which stays production-shaped.
command -v jq >/dev/null 2>&1 || { echo "TC-C2 needs jq for the WorkloadMixing patch; skipping."; exit 0; }

# --- Precondition: two ClusterQueues team-a/team-b in cohort 'research'. ---
# If absent, create queues-only (derived from experiments/06-multitenant, minus
# that experiment's k8s-native filler Job, which would skew the borrow numbers).
# Reuse whatever ResourceFlavor main-queue already uses so this is portable
# across clusters. Track creation so teardown only deletes what we made.
CREATED_QUEUES=0
if ! kubectl get clusterqueue team-a >/dev/null 2>&1 || ! kubectl get clusterqueue team-b >/dev/null 2>&1; then
  echo "team-a/team-b absent — creating cohort 'research' (queues only)."
  FLAVOR=$(kubectl get clusterqueue main-queue -o jsonpath='{.spec.resourceGroups[0].flavors[0].name}')
  cat <<YAML | kubectl apply -f -
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata: { name: team-a }
spec:
  cohort: research
  namespaceSelector: {}
  preemption: { withinClusterQueue: LowerPriority, reclaimWithinCohort: Any }
  resourceGroups:
    - coveredResources: ["cpu", "memory"]
      flavors:
        - name: ${FLAVOR}
          resources:
            - { name: cpu, nominalQuota: "6" }
            - { name: memory, nominalQuota: 24Gi }
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata: { name: team-b }
spec:
  cohort: research
  namespaceSelector: {}
  preemption: { withinClusterQueue: LowerPriority, reclaimWithinCohort: Any }
  resourceGroups:
    - coveredResources: ["cpu", "memory"]
      flavors:
        - name: ${FLAVOR}
          resources:
            - { name: cpu, nominalQuota: "6", lendingLimit: "2" }
            - { name: memory, nominalQuota: 24Gi }
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata: { name: team-a, namespace: slurm-jobs }
spec: { clusterQueue: team-a }
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata: { name: team-b, namespace: slurm-jobs }
spec: { clusterQueue: team-b }
YAML
  CREATED_QUEUES=1
fi

# --- Runtime remap (test-only): mixing -> team-b, mixing-high -> team-a. ---
WM_NS=$(kubectl get workloadmixing -A -o jsonpath='{.items[0].metadata.namespace}')
WM_NAME=$(kubectl get workloadmixing -A -o jsonpath='{.items[0].metadata.name}')
echo "Patching WorkloadMixing $WM_NS/$WM_NAME (backup saved for restore)."
kubectl get workloadmixing "$WM_NAME" -n "$WM_NS" -o json \
  | jq '{spec:{partitionMappings:.spec.partitionMappings}}' > "$EVID/wm-mappings-backup.json"

C2_LOW=""
C2_HIGH=""
restore_c2() {
  echo "=== TC-C2 teardown/restore ==="
  kubectl patch workloadmixing "$WM_NAME" -n "$WM_NS" --type=merge \
    -p "$(cat "$EVID/wm-mappings-backup.json")" 2>/dev/null || true
  [ -n "$C2_LOW" ] && kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$C2_LOW" --ignore-not-found || true
  [ -n "$C2_HIGH" ] && kubectl delete jobset -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$C2_HIGH" --ignore-not-found || true
  kubectl exec -n slurm "$LOGIN_POD" -- scancel "$C2_LOW" "$C2_HIGH" 2>/dev/null || true
  if [ "$CREATED_QUEUES" = "1" ]; then
    kubectl delete localqueue -n slurm-jobs team-a team-b --ignore-not-found || true
    kubectl delete clusterqueue team-a team-b --ignore-not-found || true
  fi
}
trap restore_c2 EXIT

# Add only the localQueue override to the mappings that already exist, keeping
# whatever workloadPriorityClass the cluster configured; append a mapping only
# when the partition is missing entirely. Overwriting the priority classes with
# hardcoded names would silently change the CR on clusters that use their own.
PATCH=$(kubectl get workloadmixing "$WM_NAME" -n "$WM_NS" -o json | jq -c '
  {spec:{partitionMappings:(
    (.spec.partitionMappings // []) as $m
    | ($m | map(.partitionName)) as $names
    | ($m | map(
        if   .partitionName == "mixing"      then . + {localQueue: "team-b"}
        elif .partitionName == "mixing-high" then . + {localQueue: "team-a"}
        else . end))
      + (if ($names | index("mixing"))      then [] else [{partitionName: "mixing",      workloadPriorityClass: "normal-priority", localQueue: "team-b"}] end)
      + (if ($names | index("mixing-high")) then [] else [{partitionName: "mixing-high", workloadPriorityClass: "high-priority",   localQueue: "team-a"}] end)
  )}}')
kubectl patch workloadmixing "$WM_NAME" -n "$WM_NS" --type=merge -p "$PATCH"

# Give the bridge's A1 hot-reload time to pick up the new mapping.
sleep 10

# Step 2: fill team-b beyond its nominal 6 CPU so it BORROWS team-a's idle quota.
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --hold --partition=mixing --ntasks=10 --cpus-per-task=1 --wrap="srun sleep 300"' | tee "$EVID/submit-low.txt"
C2_LOW=$(grep -oE '[0-9]+' "$EVID/submit-low.txt" | tail -1)
echo "C2_LOW=$C2_LOW" | tee "$EVID/low_jobid.txt"
for i in $(seq 1 12); do kubectl get jobset -n slurm-jobs -o name | grep -q "$C2_LOW" && break; sleep 5; done
kubectl exec -n slurm "$LOGIN_POD" -- scontrol release "$C2_LOW"

echo "Waiting for team-b admitted CPU usage to exceed its nominal 6 (borrowing)..."
for i in $(seq 1 24); do
  USED=$(kubectl get clusterqueue team-b -o json 2>/dev/null | jq -r '.status.flavorsUsage[]?.resources[]? | select(.name=="cpu") | .total' 2>/dev/null || true)
  echo "team-b cpu used=${USED:-unknown}"
  case "${USED:-0}" in ""|0|1|2|3|4|5|6) sleep 5 ;; *) break ;; esac
done
kubectl get clusterqueue -o custom-columns=NAME:.metadata.name,BORROW:.status.flavorsUsage | tee "$EVID/borrow.txt"

# Step 3: team-a claims its full nominal quota -> triggers within-cohort reclaim.
kubectl exec -n slurm "$LOGIN_POD" -- bash -lc 'sbatch --partition=mixing-high --ntasks=6 --cpus-per-task=1 --wrap="srun sleep 120"' | tee "$EVID/submit-high.txt"
C2_HIGH=$(grep -oE '[0-9]+' "$EVID/submit-high.txt" | tail -1)
echo "C2_HIGH=$C2_HIGH" | tee "$EVID/high_jobid.txt"

# Step 4: observe reclamation and team-a becoming Ready within 180s.
sleep 20
kubectl get events -n slurm-jobs --field-selector reason=Preempted | tee "$EVID/reclaim.txt" || true
kubectl get workloads -n slurm-jobs | tee "$EVID/workloads.txt" || true
kubectl wait --for=condition=Ready pod -n slurm-jobs -l "k8s-bridge.x-k8s.io/slurm-job-id=$C2_HIGH" --timeout=180s | tee "$EVID/team-a-ready.txt" || true

echo "Suite C script done (TC-C2 restore runs on the exit trap)."

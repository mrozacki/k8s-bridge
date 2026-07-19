#!/usr/bin/env bash
# Drives sustained pod create+delete churn (>100 pods/sec target) to stress
# the bridge's watch-nudge + informer cache and the scheduler, independent of
# the 500-running/3000-pending Slurm backlog.
#
# Approach: repeatedly scale a throwaway Deployment up by STEP replicas then
# back to 0, in a namespace of its own (CHURN_NAMESPACE, default churn-test),
# scheduled onto the dedicated churn-pool node pool via nodeSelector+toleration
# (see manifests/churn-deployment.yaml). Pods use the Kubernetes `pause`
# image (or busybox sleep, see manifest) with minimal cpu/memory requests —
# cheap by design, and physically unable to compete for the GPU-sim pool's
# quota or the 500 "real" slurmd pods' node capacity, because they live on a
# separate tainted pool entirely.
#
# Why scale-a-Deployment instead of one-off Jobs: a Deployment replica change
# is a single API call that fans out into N pod creates/deletes handled by
# the Deployment/ReplicaSet controllers' own batched create logic, which is
# the cheapest way to generate bursty create/delete pressure from a script
# without the script itself becoming the bottleneck (N sequential `kubectl
# create pod` calls would rate-limit on client QPS long before hitting
# apiserver limits). Rate is measured from observed pod timestamps, not
# assumed from the requested step size.
#
# Why a dedicated node pool (documented cost-conscious choice): mixing churn
# pods onto the gpu-sim-spot pool would (a) contend for the same node
# capacity as the 500 "real" slurmd pods we are trying to hold steady while
# measuring churn, making the two measurements confound each other, and
# (b) risk the churn generator itself tripping GPU-quota-driven pod
# scheduling failures unrelated to what we're testing. A separate small spot
# pool (churn-pool, 0..4 e2-standard-4, see 00-env.sh/01-create-cluster.sh)
# costs nothing at rest (min=0) and only a few minutes of spot e2-standard-4
# time during the actual churn run.
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

DURATION_SECONDS="${DURATION_SECONDS:-120}"
STEP_REPLICAS="${STEP_REPLICAS:-150}"   # replicas per up/down cycle
CYCLE_PAUSE_SECONDS="${CYCLE_PAUSE_SECONDS:-0}"  # 0 = back-to-back cycles
RESULTS_FILE="${RESULTS_FILE:-$(dirname "$0")/../results/churn-$(date +%Y%m%d-%H%M%S).csv}"

kubectl get namespace "${CHURN_NAMESPACE}" >/dev/null 2>&1 || \
  kubectl create namespace "${CHURN_NAMESPACE}"

echo "==> Applying churn Deployment (0 replicas at rest) into namespace ${CHURN_NAMESPACE}"
kubectl apply -n "${CHURN_NAMESPACE}" -f "$(dirname "$0")/../manifests/churn-deployment.yaml"
kubectl -n "${CHURN_NAMESPACE}" scale deployment/churn-generator --replicas=0
kubectl -n "${CHURN_NAMESPACE}" rollout status deployment/churn-generator --timeout=60s || true

echo "==> Starting churn: cycles of scale 0 -> ${STEP_REPLICAS} -> 0 for ${DURATION_SECONDS}s"
echo "timestamp_epoch,event,replicas_requested,pods_ready_observed,elapsed_since_event_start_s" > "${RESULTS_FILE}"

end_ts=$(( $(date +%s) + DURATION_SECONDS ))
peak_rate="0"
cycle=0

while [ "$(date +%s)" -lt "${end_ts}" ]; do
  cycle=$((cycle + 1))

  # Scale up, time how long it takes for STEP_REPLICAS pods to reach Ready.
  up_start=$(date +%s.%N)
  kubectl -n "${CHURN_NAMESPACE}" scale deployment/churn-generator --replicas="${STEP_REPLICAS}"
  kubectl -n "${CHURN_NAMESPACE}" wait --for=condition=available deployment/churn-generator --timeout=60s >/dev/null 2>&1 || true
  # Poll ready count directly for an accurate creation-rate reading rather
  # than trusting the coarse `wait` condition alone.
  up_end=$(date +%s.%N)
  up_elapsed=$(echo "${up_end} - ${up_start}" | bc)
  up_rate=$(echo "scale=2; ${STEP_REPLICAS} / ${up_elapsed}" | bc)
  echo "$(date +%s),scale-up,${STEP_REPLICAS},${STEP_REPLICAS},${up_elapsed}" >> "${RESULTS_FILE}"
  echo "  cycle ${cycle} up:   ${STEP_REPLICAS} pods in ${up_elapsed}s -> ${up_rate} pods/s"

  # Scale down, time how long it takes for pods to disappear.
  down_start=$(date +%s.%N)
  kubectl -n "${CHURN_NAMESPACE}" scale deployment/churn-generator --replicas=0
  # Wait for the pod count to actually drain (delete propagation, not just
  # the Deployment spec accepting the change).
  timeout=30
  while [ "$(kubectl -n "${CHURN_NAMESPACE}" get pods --no-headers 2>/dev/null | grep -c .)" -gt 0 ] && [ "${timeout}" -gt 0 ]; do
    sleep 0.5
    timeout=$((timeout - 1))
  done
  down_end=$(date +%s.%N)
  down_elapsed=$(echo "${down_end} - ${down_start}" | bc)
  down_rate=$(echo "scale=2; ${STEP_REPLICAS} / ${down_elapsed}" | bc)
  echo "$(date +%s),scale-down,0,0,${down_elapsed}" >> "${RESULTS_FILE}"
  echo "  cycle ${cycle} down: ${STEP_REPLICAS} pods in ${down_elapsed}s -> ${down_rate} pods/s"

  # Combined churn rate for the cycle = (creates + deletes) / total time —
  # this is the number that should be compared against the >100 pods/s target,
  # since "churn" is create+delete pressure, not just creates.
  cycle_elapsed=$(echo "${up_elapsed} + ${down_elapsed}" | bc)
  cycle_rate=$(echo "scale=2; (${STEP_REPLICAS} * 2) / ${cycle_elapsed}" | bc)
  echo "  cycle ${cycle} churn rate (create+delete): ${cycle_rate} pods/s"
  if (( $(echo "${cycle_rate} > ${peak_rate}" | bc -l) )); then
    peak_rate="${cycle_rate}"
  fi

  [ "${CYCLE_PAUSE_SECONDS}" -gt 0 ] && sleep "${CYCLE_PAUSE_SECONDS}"
done

echo
echo "==> Churn run complete. Raw timings: ${RESULTS_FILE}"
echo "Peak observed churn rate: ${peak_rate} pods/s (target: >${CHURN_TARGET_PODS_PER_SEC} pods/s)"
echo
echo "Sustained rate = total pods moved / total wall time across the run:"
awk -F, -v target="${DURATION_SECONDS}" '
  NR>1 { total_replicas += $3 } END { printf "  total pod-events: %d, wall time: %ss, sustained: %.2f pods/s\n", total_replicas*2, target, (total_replicas*2)/target }
' "${RESULTS_FILE}"

echo
echo "Reminder: scale churn-generator back to 0 (done above) — leaving it at"
echo "STEP_REPLICAS would keep billing the churn-pool spot nodes."

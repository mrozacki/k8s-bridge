#!/usr/bin/env bash
# Submits N held Slurm jobs (default: PENDING_TARGET from 00-env.sh, i.e.
# 3000) into a GPU-mapped partition, each requesting one simulated GPU via
# --gres, so the bridge translates each into a JobSet requesting
# nvidia.com/gpu: 1. Adapted from experiments/07-scale/scripts/backlog-slurm.sh
# (that script has no --gres and submits into the plain "mixing" partition;
# 07-scale's own README/results are untouched — this is a copy+extension,
# not a modification).
#
# Batched + rate-limited on purpose: 07-scale's own findings (see its README
# and docs/VALIDATION.md) showed slurmrestd/slurmctld
# response times degrade under a hot sbatch loop at thousands of jobs. This
# script submits in chunks with a configurable pause between chunks so it is
# safe to point at slurmrestd at 3000-job volume without hammering it in one
# burst — the same one-exec-not-N-execs trick as backlog-slurm.sh (the whole
# per-chunk loop runs inside a single `kubectl exec`, not N separate execs).
#
# Partition: this experiment expects a bridge partitionMapping entry for a
# GPU partition (see manifests/bridge-values-scale.yaml, partitionMappings:
# partitionName "mixing-gpu" -> workloadPriorityClass "normal-priority") and
# a matching Slurm partition of the same name with GresTypes=gpu configured
# (see experiments/01-gke-playground/manifests/slurm-values.yaml for the
# GresTypes/gres.conf pattern this experiment's slurm-values reuses).
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

N="${1:-${PENDING_TARGET}}"
CHUNK_SIZE="${CHUNK_SIZE:-100}"       # jobs submitted per kubectl-exec batch
CHUNK_PAUSE_SECONDS="${CHUNK_PAUSE_SECONDS:-2}"  # pause between batches
PARTITION="${GPU_PARTITION:-mixing-gpu}"
GRES="${GRES:-gpu:1}"

[[ "$N" =~ ^[0-9]+$ ]] || { echo "count must be a non-negative integer, got: $N" >&2; exit 2; }
[[ "$CHUNK_SIZE" =~ ^[0-9]+$ ]] || { echo "CHUNK_SIZE must be a non-negative integer" >&2; exit 2; }

echo "==> Submitting ${N} GPU-gres held jobs into partition '${PARTITION}' (gres=${GRES})"
echo "    in chunks of ${CHUNK_SIZE}, ${CHUNK_PAUSE_SECONDS}s pause between chunks"

submitted=0
start_ts=$(date +%s)
while [ "${submitted}" -lt "${N}" ]; do
  remaining=$(( N - submitted ))
  this_chunk=$(( remaining < CHUNK_SIZE ? remaining : CHUNK_SIZE ))
  kubectl -n slurm exec deploy/slurm-login-slinky -- bash -c \
    "for i in \$(seq 1 ${this_chunk}); do sbatch --partition=${PARTITION} --gres=${GRES} --ntasks=1 --time=5 --wrap='sleep 5' >/dev/null; done"
  submitted=$(( submitted + this_chunk ))
  elapsed=$(( $(date +%s) - start_ts ))
  echo "  submitted ${submitted}/${N} (${elapsed}s elapsed)"
  [ "${submitted}" -lt "${N}" ] && sleep "${CHUNK_PAUSE_SECONDS}"
done

echo
echo "==> Done. Current Slurm queue depth for ${PARTITION}:"
kubectl -n slurm exec deploy/slurm-login-slinky -- squeue -h -p "${PARTITION}" | wc -l

echo
echo "Cross-check against Kueue admission (should climb towards RUNNING_TARGET=${RUNNING_TARGET}"
echo "admitted, remainder PENDING, as the bridge translates held jobs into JobSets):"
echo "  kubectl get workloads -A | grep -c true   # admitted"
echo "  kubectl get workloads -A | grep -c false  # pending"

#!/usr/bin/env bash
# Submits N tiny Slurm jobs to the mixing partition (auto-held by the lua
# plugin). Runs the loop INSIDE the login pod - one exec, not N.
set -euo pipefail
N="${1:?usage: backlog-slurm.sh <count>}"
# N is interpolated into a bash -c string that runs inside the login pod; a
# non-numeric value would let arbitrary shell through. Reject anything but digits.
[[ "$N" =~ ^[0-9]+$ ]] || { echo "count must be a non-negative integer, got: $N" >&2; exit 2; }
kubectl -n slurm exec deploy/slurm-login-slinky -- bash -c \
  "for i in \$(seq 1 ${N}); do sbatch --partition=mixing --ntasks=1 --time=5 --wrap='sleep 5' >/dev/null; done; squeue -h | wc -l"

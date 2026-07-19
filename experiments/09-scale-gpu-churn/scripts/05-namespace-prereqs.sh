#!/usr/bin/env bash
# found live (2026-07-06): the live deploy discovered that
# slurmd pods (translated JobSet members) need two things to exist in the
# bridge's JobSet namespace (BRIDGE_NAMESPACE, i.e. slurm-jobs) BEFORE the
# backlog is submitted, neither of which the earlier RUN ORDER provisioned:
#
#   (a) the Slurm auth Secret, `slurm-auth-slurm` — created by the Slinky
#       chart install in the `slurm` namespace, but slurmd pods scheduled by
#       the bridge into the JobSet namespace need their own copy of it
#       mounted (see experiments/02-manual-bridge/manifests/slurmd-jobset.yaml
#       and experiments/DEMO.md's "Bridge in CRD mode" step, which shows the
#       same copy pattern by hand).
#   (b) a `wm-gres.conf` ConfigMap key so slurmd advertises the simulated GPU
#       GRES (ADR-0010, count-only: `Name=gpu Count=1`, no `File=` — the
#       count-only form validated live and reused by this
#       experiment's slurm-values-scale.yaml).
#
# Without (a), slurmd pods CrashLoop on a missing/unmounted auth Secret.
# Without (b), slurmd starts with no gpu GRES advertised, so --gres=gpu:1
# jobs in the backlog never find a node with capacity.
#
# Idempotent (kubectl apply / --dry-run=client | kubectl apply -f -), safe to
# re-run after any autoscale event or partial prior run.
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

echo "==> (a) Copying Slurm auth Secret slurm-auth-slurm: slurm -> ${BRIDGE_NAMESPACE}"
kubectl -n slurm get secret slurm-auth-slurm -o yaml \
  | sed -e "s/namespace: slurm$/namespace: ${BRIDGE_NAMESPACE}/" \
        -e '/resourceVersion:/d' -e '/uid:/d' -e '/creationTimestamp:/d' \
  | kubectl apply -f -

# (b) wm-gres-conf ConfigMap: REMOVED (L1, e2e iteration 2). The bridge no
# longer mounts a per-pod gres.conf — it broke configless slurmd registration
# (ADR-0010 revision). The count-only gres.conf is distributed by the Slurm
# chart's configFiles instead, so no per-namespace ConfigMap is needed.

echo
echo "==> Verifying"
kubectl -n "${BRIDGE_NAMESPACE}" get secret slurm-auth-slurm >/dev/null \
  && echo "    slurm-auth-slurm present in ${BRIDGE_NAMESPACE}"
echo "Namespace prerequisites ready. Run this before submitting the backlog"
echo "(scripts/20-scale-backlog.sh) — see README.md RUN ORDER."

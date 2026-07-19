# ADR-0010: Simulated accelerators as the default GPU test strategy

- **Status:** Accepted (validated live)
- **Date:** 2026-07-05

## Context

Physical-GPU E2E is blocked by a zero GPU quota in the test environment
and real accelerators are the most expensive part of any test matrix. The
owner asked whether GPUs can be simulated.

## Decision

Yes — a two-part simulation exercises the ENTIRE chain except CUDA itself,
and becomes the default for experiments and (future) e2e CI:

1. **Kubernetes side:** patch a node's status with a fake extended resource
   (`nvidia.com/gpu: 2`). Scheduler, Kueue quota, and admission treat it as
   real; pods requesting it schedule normally (no device is mounted).
2. **Slurm side:** slurmd's GRES verification needs a device entry — GPU
   type REQUIRES `File=` (count-only is rejected: "Ignoring file-less
   GPU"), and configless does NOT distribute gres.conf. The bridge mounts a
   `wm-gres-conf` ConfigMap (`Name=gpu File=/dev/null`) into slurmd's
   conf-cache for GPU jobs.

Validated end to end: `sbatch --gres=gpu:1` → JobSet with `nvidia.com/gpu`
request → Kueue admission → placement on the fake-GPU node → dynamic node
advertising `gpu:1` → Slurm GRES scheduling → completion → cleanup.

## Consequences

- GPU regressions are testable for pennies; real-GPU runs shrink to a
  hardware smoke test (driver + CUDA visibility) once quota exists.
- The gres.conf ConfigMap mount ships in the bridge (GPU jobs only);
  production swaps `/dev/null` for real device paths via the same mount.
- Two Slurm facts recorded for engineers: gpu GRES requires File=;
  configless omits gres.conf.

## Revision (2026-07-06, e2e iteration 2): the conf-cache mount broke registration

Live on Slurm 26.05 the per-pod gres.conf mount at
`/var/spool/slurmd/conf-cache/gres.conf` PREVENTED dynamic-node registration:
configless slurmd fetches its whole config from slurmctld and writes it under
`conf-cache/`, and renaming its freshly-written `gres.conf.new` over the
read-only bind-mounted `gres.conf` fails with
`_write_conf: ... Device or resource busy`, so slurmd aborts before
registering.

The original note "configless omits gres.conf" no longer holds on 26.05 — the
controller DOES distribute gres.conf to configless slurmd (that is exactly the
file slurmd was trying to write). The Slurm chart already ships a count-only
`gres.conf` (`Name=gpu Count=1`) via `configFiles`, so the per-pod mount was
redundant AND the direct cause of the failure.

**Decision:** remove the per-pod gres.conf mount from `translate.go`; rely on
the controller's configless distribution of the count-only gres.conf.
Regression: `TestToJobSetDoesNotMountIntoSlurmdConfCache`. The `wm-gres-conf`
ConfigMap and its `05-namespace-prereqs.sh` creation are no longer needed.
Rollback: re-add the mount only if a future Slurm version stops distributing
gres.conf — and then mount it OUTSIDE conf-cache (a path slurmd does not
manage), never over the file slurmd writes. **Needs a live GKE re-run to
confirm registration now completes and jobs release** (the fix removes the
proven blocker; the count-only file-less GPU acceptance still wants a live
check).
